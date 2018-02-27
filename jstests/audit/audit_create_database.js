// test that createDatabase gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_create_database';

auditTest(
    'createDatabase',
    function(m) {
        testDB = m.getDB(testDBName);
        assert.commandWorked(testDB.dropDatabase());
        assert.commandWorked(testDB.createCollection('foo'));

        beforeLoad = Date.now();
        auditColl = getAuditEventsCollection(m, testDBName);
        assert.eq(1, auditColl.count({
            atype: "createDatabase",
            ts: withinFewSecondsBefore(beforeLoad),
            'param.ns': testDBName,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { /* no special mongod options */ }
);
