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

class PartitionedCollection : public Collection
{
public:
    PartitionedCollection(OperationContext* txn,
                           const StringData& fullNS,
                           CollectionCatalogEntry* cce,  // does not own
                           RecordStore* recordStore,     // does not own
                           DatabaseCatalogEntry* dbce);  // does not own

    virtual ~PartitionedCollection();

    bool isPartitioned() const override{
        return true;
    }

    // partitioning-specific methods

    // initialization
    Status initOnCreate(OperationContext* txn) override;
    Status createPkIndexOnEmptyCollection(OperationContext* txn) override;

    uint64_t numPartitions() const override;

    // create new parttiion which never existed before
    Status createPartition(OperationContext* txn) override;
    Status createPartition(OperationContext* txn, const BSONObj& newPivot, const BSONObj &partitionInfo) override;

    // add existing partition from metadata
    Status loadPartition(OperationContext* txn, BSONObj const& pmd);

    // drop partitions
    void dropPartition(OperationContext* txn, int64_t id) override;
    void dropPartitionsLEQ(OperationContext* txn, const BSONObj &pivot) override;

private:
    void dropPartitionInternal(OperationContext* txn, int64_t id);
    BSONObj getValidatedPKFromObject(OperationContext* txn, const BSONObj &obj);
    BSONObj getPK(const BSONObj& doc) const;
    bool getMaxPKForPartitionCap(OperationContext* txn, BSONObj &result) const;

    // return upper bound
    BSONObj getUpperBound() const;

    // partition data
    struct PartitionData {
        int64_t id; //partition ID
        BSONObj maxpk; // maximum PK value

        PartitionData(int64_t _id, BSONObj const& _maxpk)
            : id(_id),
              maxpk(_maxpk) {}
        PartitionData(int64_t _id, BSONElement const& _maxpk)
            : id(_id),
              maxpk(_maxpk.Obj().getOwned()) {}
    };

    KVRecordStorePartitioned* _recordStore;

    // The primary index pattern.
    BSONObj _pkPattern;
    // Collection per partition
    std::deque<PartitionData> _partitions;

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
