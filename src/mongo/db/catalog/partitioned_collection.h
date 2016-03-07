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

#pragma once

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/partitioned_index_catalog.h"

#include <deque>

namespace mongo {

class KVRecordStorePartitioned;

class PartitionedCollection : public Collection, UpdateNotifier
{
public:
    PartitionedCollection(OperationContext* txn,
                           const StringData& fullNS,
                           CollectionCatalogEntry* cce,  // does not own
                           RecordStore* recordStore,     // does not own
                           DatabaseCatalogEntry* dbce);  // does not own

    Status initOnCreate(OperationContext* txn);

    Status createPkIndexOnEmptyCollection(OperationContext* txn);

    ~PartitionedCollection();

    bool ok() const {
        return _magic == 1357924;
    }

    CollectionCatalogEntry* getCatalogEntry() {
        return _cce;
    }
    const CollectionCatalogEntry* getCatalogEntry() const {
        return _cce;
    }

    DatabaseCatalogEntry* getDBCatalogEntry() {
        return _dbce;
    }
    const DatabaseCatalogEntry* getDBCatalogEntry() const {
        return _dbce;
    }

    CollectionInfoCache* infoCache() {
        return &_infoCache;
    }
    const CollectionInfoCache* infoCache() const {
        return &_infoCache;
    }

    const NamespaceString& ns() const {
        return _ns;
    }

    const IndexCatalog* getIndexCatalog() const {
        return &_indexCatalog;
    }
    IndexCatalog* getIndexCatalog() {
        return &_indexCatalog;
    }

    const RecordStore* getRecordStore() const;
    RecordStore* getRecordStore();

    CursorManager* getCursorManager() const {
        return &_cursorManager;
    }

    bool requiresIdIndex() const;

    Snapshotted<BSONObj> docFor(OperationContext* txn, const RecordId& loc) const;

    /**
     * @param out - contents set to the right docs if exists, or nothing.
     * @return true iff loc exists
     */
    bool findDoc(OperationContext* txn, const RecordId& loc, Snapshotted<BSONObj>* out) const;

    /**
     * Default arguments will return all items in the collection.
     */
    RecordIterator* getIterator(
        OperationContext* txn,
        const RecordId& start = RecordId(),
        const CollectionScanParams::Direction& dir = CollectionScanParams::FORWARD) const;

    /**
     * Returns many iterators that partition the Collection into many disjoint sets. Iterating
     * all returned iterators is equivalent to Iterating the full collection.
     * Caller owns all pointers in the vector.
     */
    std::vector<RecordIterator*> getManyIterators(OperationContext* txn) const;

    void deleteDocument(OperationContext* txn,
                        const RecordId& loc,
                        bool cappedOK = false,
                        bool noWarn = false,
                        BSONObj* deletedId = 0);

    /**
     * this does NOT modify the doc before inserting
     * i.e. will not add an _id field for documents that are missing it
     *
     * If enforceQuota is false, quotas will be ignored.
     */
    StatusWith<RecordId> insertDocument(OperationContext* txn,
                                        const BSONObj& doc,
                                        bool enforceQuota);

    StatusWith<RecordId> insertDocument(OperationContext* txn,
                                        const DocWriter* doc,
                                        bool enforceQuota);

    StatusWith<RecordId> insertDocument(OperationContext* txn,
                                        const BSONObj& doc,
                                        MultiIndexBlock* indexBlock,
                                        bool enforceQuota);

    /**
     * If the document at 'loc' is unlikely to be in physical memory, the storage
     * engine gives us back a RecordFetcher functor which we can invoke in order
     * to page fault on that record.
     *
     * Returns NULL if the document does not need to be fetched.
     *
     * Caller takes ownership of the returned RecordFetcher*.
     */
    RecordFetcher* documentNeedsFetch(OperationContext* txn, const RecordId& loc) const;

    /**
     * updates the document @ oldLocation with newDoc
     * if the document fits in the old space, it is put there
     * if not, it is moved
     * @return the post update location of the doc (may or may not be the same as oldLocation)
     */
    StatusWith<RecordId> updateDocument(OperationContext* txn,
                                        const RecordId& oldLocation,
                                        const Snapshotted<BSONObj>& oldDoc,
                                        const BSONObj& newDoc,
                                        bool enforceQuota,
                                        bool indexesAffected,
                                        OpDebug* debug);

    /**
     * right now not allowed to modify indexes
     */
    Status updateDocumentWithDamages(OperationContext* txn,
                                     const RecordId& loc,
                                     const Snapshotted<RecordData>& oldRec,
                                     const char* damageSource,
                                     const mutablebson::DamageVector& damages);

    StatusWith<CompactStats> compact(OperationContext* txn, const CompactOptions* options);

    /**
     * removes all documents as fast as possible
     * indexes before and after will be the same
     * as will other characteristics
     */
    Status truncate(OperationContext* txn);

    /**
     * @param full - does more checks
     * @param scanData - scans each document
     * @return OK if the validate run successfully
     *         OK will be returned even if corruption is found
     *         deatils will be in result
     */
    Status validate(OperationContext* txn,
                    bool full,
                    bool scanData,
                    ValidateResults* results,
                    BSONObjBuilder* output);

    /**
     * forces data into cache
     */
    Status touch(OperationContext* txn,
                 bool touchData,
                 bool touchIndexes,
                 BSONObjBuilder* output) const;

    void temp_cappedTruncateAfter(OperationContext* txn, RecordId end, bool inclusive);

    bool isCapped() const {
        return false;
    }

    bool isPartitioned() const {
        return true;
    }

    uint64_t numRecords(OperationContext* txn) const;

    uint64_t dataSize(OperationContext* txn) const;

    int averageObjectSize(OperationContext* txn) const {
        uint64_t n = numRecords(txn);
        if (n == 0)
            return 5;
        return static_cast<int>(dataSize(txn) / n);
    }

    uint64_t getIndexSize(OperationContext* opCtx, BSONObjBuilder* details = NULL, int scale = 1);

    // partitioning-specific methods

    // create new parttiion which never existed before
    Status createPartition(OperationContext* txn);
    Status createPartition(OperationContext* txn, const BSONObj& newPivot, const BSONObj &partitionInfo);

    // add existing partition from metadata
    Status loadPartition(OperationContext* txn, BSONObj const& pmd);

    // drop partitions
    void dropPartition(OperationContext* txn, int64_t id);
    void dropPartitionsLEQ(OperationContext* txn, const BSONObj &pivot);

    uint64_t numPartitions() const;

private:
    Status recordStoreGoingToMove(OperationContext* txn,
                                  const RecordId& oldLocation,
                                  const char* oldBuffer,
                                  size_t oldSize);

    Status recordStoreGoingToUpdateInPlace(OperationContext* txn, const RecordId& loc);

    void dropPartitionInternal(OperationContext* txn, int64_t id);
    BSONObj getValidatedPKFromObject(const BSONObj &obj) const;
    Collection* getPrttnForRecordId(const RecordId& loc) const;
    Collection* getPrttnForDoc(const BSONObj& doc) const;
    BSONObj getPK(const BSONObj& doc) const;
    bool getMaxPKForPartitionCap(OperationContext* txn, BSONObj &result) const;

    // return upper bound
    BSONObj getUpperBound() const;

    // partition data
    struct PartitionData {
        int64_t id; //partition ID
        BSONObj maxpk; // maximum PK value
        Collection* collection; //not owned

        PartitionData(int64_t _id, BSONObj const& _maxpk, Collection* _collection)
            : id(_id),
              maxpk(_maxpk),
              collection(_collection) {}
        PartitionData(int64_t _id, BSONElement const& _maxpk, Collection* _collection)
            : id(_id),
              maxpk(_maxpk.Obj().getOwned()),
              collection(_collection) {}
    };

    bool _enforceQuota(bool userEnforeQuota) const;

    int _magic;

    NamespaceString _ns;
    CollectionCatalogEntry* _cce;
    KVRecordStorePartitioned* _recordStore;
    DatabaseCatalogEntry* _dbce;
    CollectionInfoCache _infoCache;
    PartitionedIndexCatalog _indexCatalog;

    // The primary index pattern.
    BSONObj _pkPattern;
    // Collection per partition
    std::deque<PartitionData> _partitions;

    // this is mutable because read only users of the Collection class
    // use it keep state.  This seems valid as const correctness of Collection
    // should be about the data.
    mutable CursorManager _cursorManager;

};

inline void cloneBSONWithFieldChanged(BSONObjBuilder &b, const BSONObj &orig, const BSONElement &newElement, bool appendIfMissing = true) {
    StringData fieldName = newElement.fieldName();
    bool replaced = false;
    for (BSONObjIterator it(orig); it.more(); it.next()) {
        BSONElement e = *it;
        if (fieldName == e.fieldName()) {
            b.append(newElement);
            replaced = true;
        } else {
            b.append(e);
        }
    }
    if (!replaced && appendIfMissing) {
        b.append(newElement);
    }
}

inline BSONObj cloneBSONWithFieldChanged(const BSONObj &orig, const BSONElement &newElement, bool appendIfMissing = true) {
    BSONObjBuilder b(orig.objsize());
    cloneBSONWithFieldChanged(b, orig, newElement, appendIfMissing);
    return b.obj();
}

template<typename T>
void cloneBSONWithFieldChanged(BSONObjBuilder &b, const BSONObj &orig, const StringData &fieldName, const T &newValue, bool appendIfMissing = true) {
    bool replaced = false;
    for (BSONObjIterator it(orig); it.more(); it.next()) {
        BSONElement e = *it;
        if (fieldName == e.fieldName()) {
            b.append(fieldName, newValue);
            replaced = true;
        } else {
            b.append(e);
        }
    }
    if (!replaced && appendIfMissing) {
        b.append(fieldName, newValue);
    }
}

template<typename T>
BSONObj cloneBSONWithFieldChanged(const BSONObj &orig, const StringData &fieldName, const T &newValue, bool appendIfMissing = true) {
    BSONObjBuilder b(orig.objsize());
    cloneBSONWithFieldChanged(b, orig, fieldName, newValue, appendIfMissing);
    return b.obj();
}

}
