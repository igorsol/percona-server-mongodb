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

#include "mongo/db/auth/action_type.h"
#include "mongo/db/commands.h"

#include "get_partition_info_command.h"

namespace mongo {
    namespace partcoll {
        GetPartitionInfoCommand::GetPartitionInfoCommand()
            : PartitionCommandBase("getPartitionInfo") {}
            
        void GetPartitionInfoCommand::addRequiredPrivileges(const std::string& dbname,
                                                          const BSONObj& cmdObj,
                                                          std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::getPartitionInfo);
            out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
        }

        void GetPartitionInfoCommand::help(std::stringstream &h) const {
            h << "get partition information, returns BSON with number of partitions and array\n" <<
                "of partition info.\n" <<
                "Example: {getPartitionInfo:\"foo\"}";
        }

        bool GetPartitionInfoCommand::run(mongo::OperationContext* txn,
                                        const std::string &db,
                                        BSONObj &cmdObj,
                                        int options,
                                        std::string &errmsg,
                                        BSONObjBuilder &result,
                                        bool fromRepl) {
            if (!this->currentEngineSupportsPartitionedCollections()) {
                this->printEngineSupportFailure(errmsg);
                return false;
            }

            //TODO: this implementation is no-op
            errmsg = "Not implemented yet";
            return false;
        }

    }

    partcoll::GetPartitionInfoCommand getPartitionInfo;
}
