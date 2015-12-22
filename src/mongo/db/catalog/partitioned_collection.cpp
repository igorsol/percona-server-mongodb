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
#include "mongo/db/storage/kv/dictionary/kv_record_store_partitioned.h"

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

    //TODO: init _pkPattern
    //TODO: Create partitions from metadata
    //for (: getCatalogEntry()->) {
    //    _partitions->push_back(new CollectionImpl(txn, , cce, , dbce));
    //}
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

StatusWith<RecordId> PartitionedCollection::updateDocument(OperationContext* txn,
                                                const RecordId& oldLocation,
                                                const Snapshotted<BSONObj>& objOld,
                                                const BSONObj& objNew,
                                                bool enforceQuota,
                                                bool indexesAffected,
                                                OpDebug* debug) {
    raise(SIGTRAP); //TODO: implement updateDocument
    return StatusWith<RecordId>(
        ErrorCodes::InternalError, "::updateDocument is not implemented yet");
}

Status PartitionedCollection::updateDocumentWithDamages(OperationContext* txn,
                                             const RecordId& loc,
                                             const Snapshotted<RecordData>& oldRec,
                                             const char* damageSource,
                                             const mutablebson::DamageVector& damages) {
    raise(SIGTRAP); //TODO: implement updateDocumentWithDamages
    return Status(
        ErrorCodes::InternalError, "::updateDocumentWithDamages is not implemented yet");
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
    raise(SIGTRAP); //TODO: implement truncate
    return Status(
        ErrorCodes::InternalError, "::truncate is not implemented yet");
}

Status PartitionedCollection::validate(OperationContext* txn,
                            bool full,
                            bool scanData,
                            ValidateResults* results,
                            BSONObjBuilder* output) {
    raise(SIGTRAP); //TODO: implement validate
    return Status(
        ErrorCodes::InternalError, "::validate is not implemented yet");
}

Status PartitionedCollection::touch(OperationContext* txn,
                         bool touchData,
                         bool touchIndexes,
                         BSONObjBuilder* output) const {
    raise(SIGTRAP); //TODO: implement touch
    return Status(
        ErrorCodes::InternalError, "::touch is not implemented yet");
}

void PartitionedCollection::temp_cappedTruncateAfter(OperationContext* txn, RecordId end, bool inclusive) {
    massert(19175, "partitioned collection cannot be capped", isCapped());
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
    raise(SIGTRAP); //TODO: implement getIndexSize
    return 0;
//    IndexCatalog* idxCatalog = getIndexCatalog();
//
//    IndexCatalog::IndexIterator ii = idxCatalog->getIndexIterator(opCtx, true);
//
//    uint64_t totalSize = 0;
//
//    while (ii.more()) {
//        IndexDescriptor* d = ii.next();
//        IndexAccessMethod* iam = idxCatalog->getIndex(d);
//
//        long long ds = iam->getSpaceUsedBytes(opCtx);
//
//        totalSize += ds;
//        if (details) {
//            details->appendNumber(d->indexName(), ds / scale);
//        }
//    }
//
//    return totalSize;
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
    //TODO: ensure that _pkPattern contains what is required by woCompare
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

Status PartitionedCollection::createPartition(OperationContext* txn) {
    int64_t id = 0;
    if (_partitions.size() > 0) {
        id = _partitions.back().id + 1;
        id |= 0x7fffff;
        // check if we reached maximum partitions limit
        uassert(19177, "Cannot create partition. Too many partitions already exist.",
                id != _partitions.front().id);
        //TODO: update metadata for previous partition
    }
    StatusWith<RecordStore*> prs = _recordStore->createPartition(txn, id);
    if (!prs.isOK())
        return prs.getStatus();
    Collection* collection = new CollectionImpl(txn, _ns.ns(), _cce, prs.getValue(), _dbce);
    _partitions.emplace_back(id, getUpperBound(), collection);
    //TODO: create indexes on new partition
    //TODO: update metadata
    return Status::OK();
}

void PartitionedCollection::addPartition() {
    raise(SIGTRAP); //TODO: implement addPartititon
    //_partitions.emplace_back();
}

BSONObj PartitionedCollection::getUpperBound() const {
    BSONObjBuilder c(64);
    BSONObjIterator pkIter( _pkPattern );
    while( pkIter.more() ){
        BSONElement elt = pkIter.next();
        int order = elt.isNumber() ? elt.numberInt() : 1;
        if( order > 0 ){
            c.appendMaxKey( "" );
        }
        else {
            c.appendMinKey( "" );
        }
    }
    return c.obj();
}

}
