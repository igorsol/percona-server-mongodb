#pragma once

#include <deque>

#include "mongo/bson/bsonobj.h"

namespace mongo {

struct PartitionMetaData {
	typedef std::deque<PartitionMetaData> deque;
	
    PartitionMetaData(const BSONElement& pmd)
        : obj(pmd.Obj().getOwned()) {}

    BSONObj obj;
};

}
