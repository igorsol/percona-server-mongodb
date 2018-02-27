// test that dropDatabase gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_drop_database';

auditTest(
    'dropDatabase',
    function(m) {
        testDB = m.getDB(testDBName);
        assert.writeOK(testDB.getCollection('foo').insert({ a: 1 }));
        assert.commandWorked(testDB.dropDatabase());

        beforeLoad = Date.now();
        var auditColl = getAuditEventsCollection(m, testDBName);
        assert.eq(1, auditColl.count({
            atype: "dropDatabase",
            ts: withinFewSecondsBefore(beforeLoad),
            'param.ns': testDBName,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { /* no special mongod options */ }
);
