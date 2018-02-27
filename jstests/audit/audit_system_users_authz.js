// test that system.users writes get audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_system_users_authz';

auditTest(
    '{create/drop/update}User',
    function(m) {
        testDB = m.getDB(testDBName);

        // use admin DB to count matching users in adminDB.system.users
        var adminDB = m.getDB('admin');
        adminDB.auth('admin','admin');

        // enable 'auditAuthorizationSuccess' to check successful authorization events
        adminDB.runCommand({ setParameter: 1, 'auditAuthorizationSuccess': true });

        var userObj = { user: 'john', pwd: 'john', roles: [ { role:'userAdmin', db:testDBName} ] };
        testDB.createUser(userObj);

        var updateObj = { roles: [ { role:'userAdmin', db:testDBName}, { role:'dbAdmin', db:testDBName} ] }
        testDB.updateUser(userObj.user, updateObj);
        assert.eq(1, adminDB.system.users.count({ user: userObj.user, roles: updateObj.roles }),
                     "system.users update did not update role for user: " + userObj.user);

        testDB.removeUser(userObj.user);
        assert.eq(0, testDB.system.users.count({ user: userObj.user }),
                     "removeUser did not remove user:" + userObj.user);

        // disble 'auditAuthorizationSuccess' to prevent side effects of auditing getAuditEventsCollection()
        adminDB.runCommand({ setParameter: 1, 'auditAuthorizationSuccess': false });

        beforeLoad = Date.now();
        var auditColl = getAuditEventsCollection(m, testDBName);

        assert.eq(1, auditColl.count({
            atype: "createUser",
            ts: withinFewSecondsBefore(beforeLoad),
            'param.db': testDBName,
            'param.user': userObj.user,
            //'param.roles': userObj.roles,
            'param.roles': { $elemMatch: userObj.roles[0] },
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));

        assert.eq(1, auditColl.count({
            atype: "updateUser",
            ts: withinFewSecondsBefore(beforeLoad),
            'param.db': testDBName,
            'param.user': userObj.user,
            //'param.roles': updateObj.roles,
            'param.roles': { $elemMatch: updateObj.roles[0] },
            'param.roles': { $elemMatch: updateObj.roles[1] },
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));

        assert.eq(1, auditColl.count({
            atype: "dropUser",
            ts: withinFewSecondsBefore(beforeLoad),
            'param.db': testDBName,
            'param.user': userObj.user,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));

        // Successful authorization events
        // We expect events from 4 operations: insert, update, count, delete
        assert.eq(4, auditColl.count({
            atype: "authCheck",
            ts: withinFewSecondsBefore(beforeLoad),
            'param.ns': 'admin.system.users',
            result: 0, // <-- Authorization successful
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { /* no special mongod options */ }
);
