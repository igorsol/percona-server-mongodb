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

#include "mongo/db/catalog/partitioned_index_catalog.h"

#include "mongo/db/catalog/partitioned_collection.h"

#include <signal.h>  //TODO: remove when SIGTRAP debugging will be finished

namespace mongo {

PartitionedIndexCatalog::PartitionedIndexCatalog(PartitionedCollection* collection)
    : IndexCatalog(collection) {

}

PartitionedIndexCatalog::~PartitionedIndexCatalog() {

}

Status PartitionedIndexCatalog::createIndexOnEmptyCollection(OperationContext* txn, BSONObj spec) {
    raise(SIGTRAP); //TODO: just check where this is called from    
    return Status::OK();
}

}
