// kv_record_store_partitioned.cpp

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

#include <boost/scoped_ptr.hpp>

#include "mongo/db/storage/kv/dictionary/kv_record_store_partitioned.h"
#include "mongo/db/storage/kv/dictionary/kv_engine_impl.h"

#include "mongo/util/log.h"

namespace mongo {

    std::string getMetaCollectionName(const StringData &ns) {
        mongo::StackStringBuilder ss;
        ss << ns << "$$meta";
        return ss.str();
    }

    std::string getPartitionName(const StringData &ns, uint64_t partitionID) {
        mongo::StackStringBuilder ss;
        ss << ns << "$$p" << partitionID;
        return ss.str();
    }

    KVRecordStorePartitioned::KVRecordStorePartitioned(OperationContext* opCtx,
                                                       KVEngineImpl* kvEngine,
                                                       const StringData& ns,
                                                       const StringData& ident,
                                                       const CollectionOptions& options,
                                                       KVSizeStorer *sizeStorer)
        : RecordStore(ns),
          _kvEngine(kvEngine),
          _partitionOptions(options),
          _ident(ident.toString()),
          _sizeStorer(sizeStorer)
    {
        // partition options
        _partitionOptions.partitioned = false;
        _partitionOptions._partitions = nullptr;

        //TODO: remove: load partitions from metadata
#if 0
        if (options._partitions) {
            for (auto const& pmd: *options._partitions) {
                uint64_t currID = pmd.obj["_id"].numberLong();

                _partitions.push_back(kvEngine->getRecordStore(opCtx,
                                                               ns,
                                                               getPartitionName(ident, currID),
                                                               partopt));

                // extract the pivot
                _partitionPivots.push_back(RecordId(pmd.obj["max"].numberLong()));
                _partitionIDs.push_back(currID);
            }
        }
#endif        
    }

    StatusWith<RecordStore*> KVRecordStorePartitioned::createPartition(OperationContext* txn, uint64_t partitionID) {
        std::string const partIdent = getPartitionName(_ident, partitionID);
        Status status = _kvEngine->createRecordStore(txn, ns(), partIdent, _partitionOptions);
        if (!status.isOK())
            return StatusWith<RecordStore*>(status);
        RecordStore* rs = _kvEngine->getRecordStore(txn, ns(), partIdent, _partitionOptions);
        invariant(rs);
        _partitionIDs.push_back(partitionID);
        _partitions.push_back(rs);
        //TODO: do we need to registerChange here (see KVDatabaseCatalogEntry::createCollection)
        return StatusWith<RecordStore*>(rs);
    }

    KVRecordStorePartitioned::~KVRecordStorePartitioned() {
        for (RecordStore* rs: _partitions) {
            delete rs;
        }
    }

    long long KVRecordStorePartitioned::dataSize( OperationContext* txn ) const {
        long long result = 0;
        for (RecordStore* rs: _partitions) {
            result += rs->dataSize(txn);
        }
        return result;
    }

    long long KVRecordStorePartitioned::numRecords( OperationContext* txn ) const {
        long long result = 0;
        for (RecordStore* rs: _partitions) {
            result += rs->numRecords(txn);
        }
        return result;
    }

    int64_t KVRecordStorePartitioned::storageSize( OperationContext* txn,
                                                   BSONObjBuilder* extraInfo,
                                                   int infoLevel) const {
        int64_t result = 0;
        for (RecordStore* rs: _partitions) {
            result += rs->storageSize(txn, extraInfo, infoLevel);
        }
        return result;
    }

    RecordData KVRecordStorePartitioned::dataFor( OperationContext* txn, const RecordId& loc ) const {
        return rsForRecordId(loc)->dataFor(txn, loc);
    }

    bool KVRecordStorePartitioned::findRecord( OperationContext* txn,
                                               const RecordId& loc,
                                               RecordData* out,
                                               bool skipPessimisticLocking) const {
        return rsForRecordId(loc)->findRecord(txn, loc, out, skipPessimisticLocking);
    }

    void KVRecordStorePartitioned::deleteRecord( OperationContext* txn, const RecordId& dl ) {
        rsForRecordId(dl)->deleteRecord(txn, dl);
    }

    StatusWith<RecordId> KVRecordStorePartitioned::insertRecord( OperationContext* txn,
                                                                 const char* data,
                                                                 int len,
                                                                 bool enforceQuota ) {
        // new generated record ID will always go into the last partition
        return _partitions.back()->insertRecord(txn, data, len, enforceQuota);
    }

    StatusWith<RecordId> KVRecordStorePartitioned::insertRecord( OperationContext* txn,
                                                                 const DocWriter* doc,
                                                                 bool enforceQuota ) {
        Slice value(doc->documentSize());
        doc->writeDocument(value.mutableData());
        return insertRecord(txn, value.data(), value.size(), enforceQuota);
    }

    StatusWith<RecordId> KVRecordStorePartitioned::updateRecord( OperationContext* txn,
                                                                 const RecordId& oldLocation,
                                                                 const char* data,
                                                                 int len,
                                                                 bool enforceQuota,
                                                                 UpdateNotifier* notifier ) {
        //TODO: implement
        return StatusWith<RecordId>(Status(ErrorCodes::InternalError, "update is not implemented yet"));
    }

    Status KVRecordStorePartitioned::updateWithDamages( OperationContext* txn,
                                                        const RecordId& loc,
                                                        const RecordData& oldRec,
                                                        const char* damageSource,
                                                        const mutablebson::DamageVector& damages ) {
        //TODO: implement
        return Status(ErrorCodes::InternalError, "update is not implemented yet");
    }

    RecordIterator* KVRecordStorePartitioned::getIterator( OperationContext* txn,
                                                           const RecordId& start,
                                                           const CollectionScanParams::Direction& dir) const {
        //TODO: implement similar to KVRecordStore::KVRecordIterator
        // below is broken implementation (iterates over last partition only)
        return _partitions.back()->getIterator(txn, start, dir);
    }

    std::vector<RecordIterator *> KVRecordStorePartitioned::getManyIterators( OperationContext* txn ) const {
        //TODO: implement
        std::vector<RecordIterator *> result;
        for (RecordStore* rs: _partitions) {
            std::vector<RecordIterator *> tmp = rs->getManyIterators(txn);
            result.insert(result.end(), tmp.begin(), tmp.end());
        }
        return result;
    }

    Status KVRecordStorePartitioned::truncate( OperationContext* txn ) {
        // KVRecordStore::truncate always returns Status::OK()
        // So this implementation does the same
        for (RecordStore* rs: _partitions) {
            rs->truncate(txn);
        }
        return Status::OK();
    }

    Status KVRecordStorePartitioned::validate( OperationContext* txn,
                                               bool full, bool scanData,
                                               ValidateAdaptor* adaptor,
                                               ValidateResults* results, BSONObjBuilder* output ) {
        // copy of KVRecordStore::validate
        bool invalidObject = false;
        long long numRecords = 0;
        long long dataSizeTotal = 0;
        for (boost::scoped_ptr<RecordIterator> iter( getIterator( txn ) );
             !iter->isEOF(); ) {
            numRecords++;
            if (scanData) {
                RecordData data = dataFor( txn, iter->curr() );
                size_t dataSize;
                if (full) {
                    const Status status = adaptor->validate( data, &dataSize );
                    if (!status.isOK()) {
                        results->valid = false;
                        if ( invalidObject ) {
                            results->errors.push_back("invalid object detected (see logs)");
                        }
                        invalidObject = true;
                        log() << "Invalid object detected in " << _ns << ": " << status.reason();
                    }
                    dataSizeTotal += static_cast<long long>(dataSize);
                }
            }
            iter->getNext();
        }

        if (_sizeStorer && full && scanData && results->valid) {
            if (numRecords != _numRecords.load() || dataSizeTotal != _dataSize.load()) {
                warning() << ns() << ": Existing record and data size counters ("
                          << _numRecords.load() << " records " << _dataSize.load() << " bytes) "
                          << "are inconsistent with full validation results ("
                          << numRecords << " records " << dataSizeTotal << " bytes). "
                          << "Updating counters with new values.";
            }

            _numRecords.store(numRecords);
            _dataSize.store(dataSizeTotal);

            long long oldNumRecords;
            long long oldDataSize;
            _sizeStorer->load(_ident, &oldNumRecords, &oldDataSize);
            if (numRecords != oldNumRecords || dataSizeTotal != oldDataSize) {
                warning() << ns() << ": Existing data in size storer ("
                          << oldNumRecords << " records " << oldDataSize << " bytes) "
                          << "is inconsistent with full validation results ("
                          << numRecords << " records " << dataSizeTotal << " bytes). "
                          << "Updating size storer with new values.";
            }

            _sizeStorer->store(this, _ident, numRecords, dataSizeTotal);
        }

        output->appendNumber("nrecords", numRecords);

        return Status::OK();
    }

    void KVRecordStorePartitioned::appendCustomStats( OperationContext* txn,
                                                      BSONObjBuilder* result,
                                                      double scale ) const {
        result->appendBool("capped", false);
        result->appendBool("partitioned", true);
        BSONArrayBuilder ab(result->subarrayStart("partitions"));
        for (RecordStore* rs: _partitions) {
            BSONObjBuilder b(ab.subobjStart());
            rs->appendCustomStats(txn, &b, scale);
            b.doneFast();
        }
        ab.doneFast();
    }

    void KVRecordStorePartitioned::updateStatsAfterRepair(OperationContext* txn,
                                                          long long numRecords,
                                                          long long dataSize) {
        //TODO: implement
    }

    RecordStore* KVRecordStorePartitioned::rsForRecordId(const RecordId& loc) const {
        // get partition ID from RecordID
        const int64_t partitionID = loc.partitionId();
        // if there is one partition, then the answer is easy
        if (_partitions.size() == 1) {
            invariant(partitionID == _partitionIDs[0]);
            return _partitions[0];
        }
        // first check the last partition, as we expect many inserts and
        //queries to go there
        if (partitionID == _partitionIDs.back()) {
            return _partitions.back();
        }
        // search through the whole list
        auto low = std::lower_bound(
            _partitionIDs.begin(),
            _partitionIDs.end(),
            partitionID);
        return _partitions[low - _partitionIDs.begin()];
    }

} // namespace mongo
