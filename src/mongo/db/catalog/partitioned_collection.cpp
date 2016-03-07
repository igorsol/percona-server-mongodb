/* -*- mode: C++; c-basic-offset: 4; indent-tabs-mode: nil -*- */
// vim: ft=cpp:expandtab:ts=8:sw=4:softtabstop=4:
/*======
This file is part of Percona Server for MongoDB.

Copyright (c) 2006, 2015, Percona and/or its affiliates. All rights reserved.

    Percona Server for MongoDB is free software: you can redistribute
    it and/or modify it under the terms of the GNU Affero General
    Public License, version 3, as published by the Free Software
    Foundation.

    Percona Server for MongoDB is distributed in the hope that it will
    be useful, but WITHOUT ANY WARRANTY; without even the implied
    warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
    See the GNU Affero General Public License for more details.

    You should have received a copy of the GNU Affero General Public
    License along with Percona Server for MongoDB.  If not, see
    <http://www.gnu.org/licenses/>.
======= */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/db/catalog/partitioned_collection.h"

#include "mongo/base/counter.h"
#include "mongo/base/owned_pointer_map.h"
#include "mongo/db/curop.h"
#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/storage/mmap_v1/mmap_v1_options.h"
#include "mongo/db/storage/kv/dictionary/kv_record_store_partitioned.h"

#include "mongo/util/log.h"
#include <signal.h>  //TODO: remove when SIGTRAP debugging will be finished

namespace mongo {

using boost::scoped_ptr;
using std::endl;
using std::string;
using std::vector;

using logger::LogComponent;

PartitionedCollection::PartitionedCollection(OperationContext* txn,
                                             const StringData& fullNS,
                                             CollectionCatalogEntry* cce,
                                             RecordStore* recordStore,
                                             DatabaseCatalogEntry* dbce)
    : _ns(fullNS),
      _cce(cce),
      _recordStore(recordStore->as<KVRecordStorePartitioned>()),
      _dbce(dbce),
      _infoCache(this),
      _indexCatalog(this),
      _cursorManager(fullNS) {
    _magic = 1357924;
    _indexCatalog.init(txn);
    invariant(!isCapped());
    _infoCache.reset(txn);

    // init _pkPattern
    auto opts = _cce->getCollectionOptions(txn);
    if (opts.primaryKey.isEmpty())
        _pkPattern = BSON("_id" << 1);
    else
        _pkPattern = opts.primaryKey;

    // Create partitions from metadata
    Status status = _cce->forEachPMD(txn, [this, txn](BSONObj const& pmd){return loadPartition(txn, pmd);});
    invariant(status.isOK());
}

Status PartitionedCollection::initOnCreate(OperationContext* txn) {
    return createPartition(txn);
}

Status PartitionedCollection::createPkIndexOnEmptyCollection(OperationContext* txn) {
    // if _pkPattern equals standard Id index then no need to create another one
    if (_pkPattern == BSON("_id" << 1))
        return Status::OK();
    return _indexCatalog.createIndexOnEmptyCollection(txn, _pkPattern);
}

PartitionedCollection::~PartitionedCollection() {
    for (auto& p: _partitions) {
        delete p.collection;
    }
    verify(ok());
    _magic = 0;
}

const RecordStore* PartitionedCollection::getRecordStore() const {
    return _recordStore;
}

RecordStore* PartitionedCollection::getRecordStore() {
    return _recordStore;
}

bool PartitionedCollection::requiresIdIndex() const {
    if (_ns.ns().find('$') != string::npos) {
        // no indexes on indexes
        return false;
    }

    if (_ns.isSystem()) {
        StringData shortName = _ns.coll().substr(_ns.coll().find('.') + 1);
        if (shortName == "indexes" || shortName == "namespaces" || shortName == "profile") {
            return false;
        }
    }

    if (_ns.db() == "local") {
        if (_ns.coll().startsWith("oplog."))
            return false;
    }

    if (!_ns.isSystem()) {
        // non system collections definitely have an _id index
        return true;
    }


    return true;
}

Snapshotted<BSONObj> PartitionedCollection::docFor(OperationContext* txn, const RecordId& loc) const {
    auto coll = getPrttnForRecordId(loc);
    invariant(coll);
    return coll->docFor(txn, loc);
}

bool PartitionedCollection::findDoc(OperationContext* txn,
                                     const RecordId& loc,
                                     Snapshotted<BSONObj>* out) const {
    auto coll = getPrttnForRecordId(loc);
    invariant(coll);
    return coll->findDoc(txn, loc, out);
}

RecordIterator* PartitionedCollection::getIterator(OperationContext* txn,
                                        const RecordId& start,
                                        const CollectionScanParams::Direction& dir) const {
    dassert(txn->lockState()->isCollectionLockedForMode(ns().toString(), MODE_IS));
    invariant(ok());

    return _recordStore->getIterator(txn, start, dir);
}

vector<RecordIterator*> PartitionedCollection::getManyIterators(OperationContext* txn) const {
    dassert(txn->lockState()->isCollectionLockedForMode(ns().toString(), MODE_IS));

    return _recordStore->getManyIterators(txn);
}

void PartitionedCollection::deleteDocument(
    OperationContext* txn, const RecordId& loc, bool cappedOK, bool noWarn, BSONObj* deletedId) {
    auto coll = getPrttnForRecordId(loc);
    invariant(coll);
    return coll->deleteDocument(txn, loc, cappedOK, noWarn, deletedId);
}

StatusWith<RecordId> PartitionedCollection::insertDocument(OperationContext* txn,
                                                           const DocWriter* doc,
                                                           bool enforceQuota) {
    // This implementation utilizes the fact that Collection::insertDocument version
    // ensures there are no indexes (no PK, nothing). That means we can always write
    // to the last partition. 
    // btw the only DocWriter descendant seems to be OplogDocWriter
    
    return _partitions.back().collection->insertDocument(txn, doc, enforceQuota);
}

StatusWith<RecordId> PartitionedCollection::insertDocument(OperationContext* txn,
                                                           const BSONObj& docToInsert,
                                                           bool enforceQuota) {
    auto coll = getPrttnForDoc(docToInsert);
    invariant(coll);
    return coll->insertDocument(txn, docToInsert, enforceQuota);
}

StatusWith<RecordId> PartitionedCollection::insertDocument(OperationContext* txn,
                                                           const BSONObj& doc,
                                                           MultiIndexBlock* indexBlock,
                                                           bool enforceQuota) {
    auto coll = getPrttnForDoc(doc);
    invariant(coll);
    return coll->insertDocument(txn, doc, indexBlock, enforceQuota);
}

RecordFetcher* PartitionedCollection::documentNeedsFetch(OperationContext* txn, const RecordId& loc) const {
    auto coll = getPrttnForRecordId(loc);
    invariant(coll);
    return coll->documentNeedsFetch(txn, loc);
}

namespace coll {
extern Counter64 moveCounter;
}

StatusWith<RecordId> PartitionedCollection::updateDocument(OperationContext* txn,
                                                const RecordId& oldLocation,
                                                const Snapshotted<BSONObj>& objOld,
                                                const BSONObj& objNew,
                                                bool enforceQuota,
                                                bool indexesAffected,
                                                OpDebug* debug) {
    dassert(txn->lockState()->isCollectionLockedForMode(ns().toString(), MODE_IX));
    invariant(objOld.snapshotId() == txn->recoveryUnit()->getSnapshotId());

    SnapshotId sid = txn->recoveryUnit()->getSnapshotId();

    BSONElement oldId = objOld.value()["_id"];
    if (!oldId.eoo() && (oldId != objNew["_id"]))
        return StatusWith<RecordId>(
            ErrorCodes::InternalError, "in Collection::updateDocument _id mismatch", 13596);

    // At the end of this step, we will have a map of UpdateTickets, one per index, which
    // represent the index updates needed to be done, based on the changes between objOld and
    // objNew.
    OwnedPointerMap<IndexDescriptor*, UpdateTicket> updateTickets;
    if (indexesAffected) {
        IndexCatalog::IndexIterator ii = _indexCatalog.getIndexIterator(txn, true);
        while (ii.more()) {
            IndexDescriptor* descriptor = ii.next();
            IndexAccessMethod* iam = _indexCatalog.getIndex(descriptor);

            InsertDeleteOptions options;
            options.logIfError = false;
            options.dupsAllowed =
                !(KeyPattern::isIdKeyPattern(descriptor->keyPattern()) || descriptor->unique()) ||
                repl::getGlobalReplicationCoordinator()->shouldIgnoreUniqueIndex(descriptor);
            UpdateTicket* updateTicket = new UpdateTicket();
            updateTickets.mutableMap()[descriptor] = updateTicket;
            Status ret = iam->validateUpdate(
                txn, objOld.value(), objNew, oldLocation, options, updateTicket);
            if (!ret.isOK()) {
                return StatusWith<RecordId>(ret);
            }
        }
    }

    // This can call back into Collection::recordStoreGoingToMove.  If that happens, the old
    // object is removed from all indexes.
    StatusWith<RecordId> newLocation = _recordStore->updateRecord(
        txn, oldLocation, objNew.objdata(), objNew.objsize(), _enforceQuota(enforceQuota), this);

    if (!newLocation.isOK()) {
        return newLocation;
    }

    // At this point, the old object may or may not still be indexed, depending on if it was
    // moved. If the object did move, we need to add the new location to all indexes.
    if (newLocation.getValue() != oldLocation) {
        if (debug) {
            if (debug->nmoved == -1)  // default of -1 rather than 0
                debug->nmoved = 1;
            else
                debug->nmoved += 1;
        }

        Status s = _indexCatalog.indexRecord(txn, objNew, newLocation.getValue());
        if (!s.isOK())
            return StatusWith<RecordId>(s);
        invariant(sid == txn->recoveryUnit()->getSnapshotId());
        return newLocation;
    }

    // Object did not move.  We update each index with each respective UpdateTicket.

    if (debug)
        debug->keyUpdates = 0;

    if (indexesAffected) {
        IndexCatalog::IndexIterator ii = _indexCatalog.getIndexIterator(txn, true);
        while (ii.more()) {
            IndexDescriptor* descriptor = ii.next();
            IndexAccessMethod* iam = _indexCatalog.getIndex(descriptor);

            int64_t updatedKeys;
            Status ret = iam->update(txn, *updateTickets.mutableMap()[descriptor], &updatedKeys);
            if (!ret.isOK())
                return StatusWith<RecordId>(ret);
            if (debug)
                debug->keyUpdates += updatedKeys;
        }
    }

    invariant(sid == txn->recoveryUnit()->getSnapshotId());
    return newLocation;
}

Status PartitionedCollection::recordStoreGoingToMove(OperationContext* txn,
                                          const RecordId& oldLocation,
                                          const char* oldBuffer,
                                          size_t oldSize) {
    coll::moveCounter.increment();
    _cursorManager.invalidateDocument(txn, oldLocation, INVALIDATION_DELETION);
    _indexCatalog.unindexRecord(txn, BSONObj(oldBuffer), oldLocation, true);
    return Status::OK();
}

Status PartitionedCollection::recordStoreGoingToUpdateInPlace(OperationContext* txn, const RecordId& loc) {
    // Broadcast the mutation so that query results stay correct.
    _cursorManager.invalidateDocument(txn, loc, INVALIDATION_MUTATION);
    return Status::OK();
}


Status PartitionedCollection::updateDocumentWithDamages(OperationContext* txn,
                                             const RecordId& loc,
                                             const Snapshotted<RecordData>& oldRec,
                                             const char* damageSource,
                                             const mutablebson::DamageVector& damages) {
    dassert(txn->lockState()->isCollectionLockedForMode(ns().toString(), MODE_IX));
    invariant(oldRec.snapshotId() == txn->recoveryUnit()->getSnapshotId());

    // Broadcast the mutation so that query results stay correct.
    _cursorManager.invalidateDocument(txn, loc, INVALIDATION_MUTATION);

    return _recordStore->updateWithDamages(txn, loc, oldRec.value(), damageSource, damages);
}

//TODO: move to collection_compact.cpp
StatusWith<CompactStats> PartitionedCollection::compact(OperationContext* txn,
                                             const CompactOptions* compactOptions) {
    dassert(txn->lockState()->isCollectionLockedForMode(ns().toString(), MODE_X));

    if (!_recordStore->compactSupported())
        return StatusWith<CompactStats>(ErrorCodes::CommandNotSupported,
                                        str::stream()
                                            << "cannot compact collection with record store: "
                                            << _recordStore->name());

    raise(SIGTRAP); //TODO: implement compact
    return StatusWith<CompactStats>(
        ErrorCodes::InternalError, "::compact is not implemented yet");
}

/**
 * non-partitioned collection uses this algorithm:
 * 1) store index specs
 * 2) drop indexes
 * 3) truncate record store
 * 4) re-write indexes
 */
Status PartitionedCollection::truncate(OperationContext* txn) {
    dassert(txn->lockState()->isCollectionLockedForMode(ns().toString(), MODE_X));
    massert(19190, "index build in progress", _indexCatalog.numIndexesInProgress(txn) == 0);

    // 1) store index specs
    vector<BSONObj> indexSpecs;
    {
        IndexCatalog::IndexIterator ii = _indexCatalog.getIndexIterator(txn, false);
        while (ii.more()) {
            const IndexDescriptor* idx = ii.next();
            indexSpecs.push_back(idx->infoObj().getOwned());
        }
    }

    // 2) drop indexes
    Status status = _indexCatalog.dropAllIndexes(txn, true);
    if (!status.isOK())
        return status;
    _cursorManager.invalidateAll(false);
    _infoCache.reset(txn);

    // 3) truncate record store
    status = _recordStore->truncate(txn);
    if (!status.isOK())
        return status;

    // 4) re-create indexes
    for (size_t i = 0; i < indexSpecs.size(); i++) {
        status = _indexCatalog.createIndexOnEmptyCollection(txn, indexSpecs[i]);
        if (!status.isOK())
            return status;
    }

    return Status::OK();
}

namespace {
class MyValidateAdaptor : public ValidateAdaptor {
public:
    virtual ~MyValidateAdaptor() {}

    virtual Status validate(const RecordData& record, size_t* dataSize) {
        BSONObj obj = record.toBson();
        const Status status = validateBSON(obj.objdata(), obj.objsize());
        if (status.isOK())
            *dataSize = obj.objsize();
        return Status::OK();
    }
};
}

Status PartitionedCollection::validate(OperationContext* txn,
                            bool full,
                            bool scanData,
                            ValidateResults* results,
                            BSONObjBuilder* output) {
    dassert(txn->lockState()->isCollectionLockedForMode(ns().toString(), MODE_IS));

    MyValidateAdaptor adaptor;
    Status status = _recordStore->validate(txn, full, scanData, &adaptor, results, output);
    if (!status.isOK())
        return status;

    {  // indexes
        output->append("nIndexes", _indexCatalog.numIndexesReady(txn));
        int idxn = 0;
        try {
            // Only applicable when 'full' validation is requested.
            boost::scoped_ptr<BSONObjBuilder> indexDetails(full ? new BSONObjBuilder() : NULL);
            BSONObjBuilder indexes;  // not using subObjStart to be exception safe

            IndexCatalog::IndexIterator i = _indexCatalog.getIndexIterator(txn, false);
            while (i.more()) {
                const IndexDescriptor* descriptor = i.next();
                log(LogComponent::kIndex) << "validating index " << descriptor->indexNamespace()
                                          << endl;
                IndexAccessMethod* iam = _indexCatalog.getIndex(descriptor);
                invariant(iam);

                boost::scoped_ptr<BSONObjBuilder> bob(
                    indexDetails.get() ? new BSONObjBuilder(indexDetails->subobjStart(
                                             descriptor->indexNamespace()))
                                       : NULL);

                int64_t keys;
                iam->validate(txn, full, &keys, bob.get());
                indexes.appendNumber(descriptor->indexNamespace(), static_cast<long long>(keys));

                if (bob) {
                    BSONObj obj = bob->done();
                    BSONElement valid = obj["valid"];
                    if (valid.ok() && !valid.trueValue()) {
                        results->valid = false;
                    }
                }
                idxn++;
            }

            output->append("keysPerIndex", indexes.done());
            if (indexDetails.get()) {
                output->append("indexDetails", indexDetails->done());
            }
        } catch (DBException& exc) {
            string err = str::stream() << "exception during index validate idxn "
                                       << BSONObjBuilder::numStr(idxn) << ": " << exc.toString();
            results->errors.push_back(err);
            results->valid = false;
        }
    }

    return Status::OK();
}

Status PartitionedCollection::touch(OperationContext* txn,
                         bool touchData,
                         bool touchIndexes,
                         BSONObjBuilder* output) const {
    if (touchData) {
        BSONObjBuilder b;
        Status status = _recordStore->touch(txn, &b);
        if (!status.isOK())
            return status;
        output->append("data", b.obj());
    }

    if (touchIndexes) {
        Timer t;
        IndexCatalog::IndexIterator ii = _indexCatalog.getIndexIterator(txn, false);
        while (ii.more()) {
            const IndexDescriptor* desc = ii.next();
            const IndexAccessMethod* iam = _indexCatalog.getIndex(desc);
            Status status = iam->touch(txn);
            if (!status.isOK())
                return status;
        }

        output->append("indexes",
                       BSON("num" << _indexCatalog.numIndexesTotal(txn) << "millis" << t.millis()));
    }

    return Status::OK();
}

void PartitionedCollection::temp_cappedTruncateAfter(OperationContext* txn, RecordId end, bool inclusive) {
    massert(19175, "partitioned collection cannot be capped", isCapped());
}

bool PartitionedCollection::_enforceQuota(bool userEnforeQuota) const {
    if (!userEnforeQuota)
        return false;

    if (!mmapv1GlobalOptions.quota)
        return false;

    if (_ns.db() == "local")
        return false;

    if (_ns.isSpecial())
        return false;

    return true;
}

uint64_t PartitionedCollection::numRecords(OperationContext* txn) const {
    uint64_t result = 0;
    for (auto const& pd: _partitions) {
        result += pd.collection->numRecords(txn);
    }
    return result;
}

uint64_t PartitionedCollection::dataSize(OperationContext* txn) const {
    uint64_t result = 0;
    for (auto const& pd: _partitions) {
        result += pd.collection->dataSize(txn);
    }
    return result;
}

uint64_t PartitionedCollection::getIndexSize(OperationContext* opCtx, BSONObjBuilder* details, int scale) {
    IndexCatalog* idxCatalog = getIndexCatalog();

    IndexCatalog::IndexIterator ii = idxCatalog->getIndexIterator(opCtx, true);

    uint64_t totalSize = 0;

    while (ii.more()) {
        IndexDescriptor* d = ii.next();
        IndexAccessMethod* iam = idxCatalog->getIndex(d);

        long long ds = iam->getSpaceUsedBytes(opCtx);

        totalSize += ds;
        if (details) {
            details->appendNumber(d->indexName(), ds / scale);
        }
    }

    return totalSize;
}


Collection* PartitionedCollection::getPrttnForRecordId(const RecordId& loc) const {
    const int64_t id = loc.partitionId();
    // if there is one partition, then the answer is easy
    if (_partitions.size() == 1) {
        invariant(_partitions[0].id == id);
        return _partitions[0].collection;
    }
    // first check the last partition, as we expect many inserts and
    // queries to go there
    if (id == _partitions[_partitions.size() - 1].id) {
        return _partitions.back().collection;
    }
    // search through the whole list
    auto low = std::lower_bound(_partitions.begin(), _partitions.end(), id,
                                [](const PartitionData& pd, int64_t id){return pd.id < id;});
    return low->collection;
}

Collection* PartitionedCollection::getPrttnForDoc(const BSONObj& doc) const {
    // if there is one partition, then the answer is easy
    if (_partitions.size() == 1) {
        return _partitions[0].collection;
    }
    // get PK
    BSONObj pk = getPK(doc);
    // first check the last partition, as we expect many inserts and
    // queries to go there
    if (_partitions[_partitions.size()-2].maxpk.woCompare(pk, _pkPattern) < 0) {
        return _partitions.back().collection;
    }
    // search through the whole list
    auto low = std::lower_bound(_partitions.begin(), _partitions.end(), pk,
                                [this](const PartitionData& pd, const BSONObj& pk){
                                    return pd.maxpk.woCompare(pk, this->_pkPattern) < 0;});
    return low->collection;
}

BSONObj PartitionedCollection::getPK(const BSONObj& doc) const {
    BSONObjBuilder result(64);
    BSONObjIterator pkIT(_pkPattern);
    while (pkIT.more()) {
        BSONElement field = doc[(*pkIT).fieldName()];
        if (field.eoo())
            result.appendNull((*pkIT).fieldName());
        else
            result.append(field);
        pkIT.next();
    }
    return result.obj();
}

bool PartitionedCollection::getMaxPKForPartitionCap(OperationContext* txn, BSONObj &result) const {
    auto desc = _indexCatalog.findIndexByKeyPattern(txn, _pkPattern);
    invariant(desc);
    auto iam = _indexCatalog.getIndex(desc);
    invariant(iam);
    return iam->getMaxKeyFromLastPartition(txn, result);
}

Status PartitionedCollection::createPartition(OperationContext* txn) {
    int64_t id = 0;
    BSONObj maxpkforprev;
    if (_partitions.size() > 0) {
        id = _partitions.back().id + 1;
        id &= 0x7fffff;
        // check if we reached maximum partitions limit
        uassert(19177, "Cannot create partition. Too many partitions already exist.",
                id != _partitions.front().id);
        // get maxpkforprev from last partition
        bool foundLast = getMaxPKForPartitionCap(txn, maxpkforprev);
        uassert(19189, "can only cap a partition with no pivot if it is non-empty", foundLast);
    }
    StatusWith<RecordStore*> prs = _recordStore->createPartition(txn, id);
    if (!prs.isOK())
        return prs.getStatus();
    Collection* collection = new coll::Collection(txn, _ns.ns(), _cce, prs.getValue(), _dbce);
    // update partition metadata structures
    if (_partitions.size() > 0) {
        _partitions.back().maxpk = maxpkforprev;
    }
    _partitions.emplace_back(id, getUpperBound(), collection);
    _cce->storeNewPartitionMetadata(txn, maxpkforprev, id, _partitions.back().maxpk);
    return Status::OK();
}

Status PartitionedCollection::createPartition(OperationContext*txn, const BSONObj& newPivot, const BSONObj &partitionInfo) {
    //TODO: implement
    return Status::OK();
}

// Input: BSONObj with partition metadata
// - partition Id
// - maximim value of PK
Status PartitionedCollection::loadPartition(OperationContext* txn, BSONObj const& pmd) {
    const int64_t id = pmd["_id"].numberLong();
    StatusWith<RecordStore*> prs = _recordStore->createPartition(txn, id);
    if (!prs.isOK())
        return prs.getStatus();
    Collection* collection = new coll::Collection(txn, _ns.ns(), _cce, prs.getValue(), _dbce);
    _partitions.emplace_back(id, pmd["max"], collection);
    return Status::OK();
}

void PartitionedCollection::dropPartitionInternal(OperationContext* txn, int64_t id) {
    for (auto it = _partitions.begin(); it != _partitions.end(); ++it) {
        if (it->id == id) {
            _partitions.erase(it);
            break;
        }
    }
    _cce->dropPartitionMetadata(txn, id);

    IndexCatalog::IndexIterator ii = _indexCatalog.getIndexIterator(txn, true);
    while (ii.more()) {
        IndexDescriptor* descriptor = ii.next();
        IndexAccessMethod* iam = _indexCatalog.getIndex(descriptor);
        iam->dropPartition(txn, id);
    }

    _recordStore->dropPartition(txn, id);
}

void PartitionedCollection::dropPartition(OperationContext* txn, int64_t id) {
    uassert(19188, "cannot drop partition if only one exists", numPartitions() > 1);
    dropPartitionInternal(txn, id);
}

BSONObj PartitionedCollection::getValidatedPKFromObject(const BSONObj &obj) const {
    //TODO: this is stub implementattion
    const BSONObj pk = obj.getOwned();
    return pk;
}

void PartitionedCollection::dropPartitionsLEQ(OperationContext* txn, const BSONObj &pivot) {
    BSONObj key = getValidatedPKFromObject(pivot);
    while (numPartitions() > 1 &&
           key.woCompare(_partitions[0].maxpk, _pkPattern) >= 0) {
        dropPartition(txn, _partitions[0].id);
    }
}

uint64_t PartitionedCollection::numPartitions() const {
    return _partitions.size();
}

BSONObj PartitionedCollection::getUpperBound() const {
    BSONObjBuilder c(64);
    BSONObjIterator pkIter( _pkPattern );
    while( pkIter.more() ){
        BSONElement elt = pkIter.next();
        int order = elt.isNumber() ? elt.numberInt() : 1;
        if( order > 0 ){
            c.appendMaxKey( elt.fieldName() );
        }
        else {
            c.appendMinKey( elt.fieldName() );
        }
    }
    return c.obj();
}

}
