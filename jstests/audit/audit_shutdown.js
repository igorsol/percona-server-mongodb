// test that shutdownServer gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_shutdown';

auditTest(
    'shutdown',
    function(m, restartServer) {
        m.getDB('admin').shutdownServer();
        m = restartServer();

        beforeLoad = Date.now();
        auditColl = getAuditEventsCollection(m, testDBName);
        assert.eq(1, auditColl.count({
            atype: "shutdown",
            // Give 10 seconds of slack in case shutdown / restart was particularly slow
            ts: withinFewSecondsBefore(beforeLoad, 10),
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { /* no special mongod options */ }
);
