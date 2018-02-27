// test that createDatabase gets audited

if (TestData.testData !== undefined) {
    load(TestData.testData + '/audit/_audit_helpers.js');
} else {
    load('jstests/audit/_audit_helpers.js');
}

var testDBName = 'audit_log_application_message';

auditTest(
    'logApplicationMessage',
    function(m) {
        var msg = "it's a trap!"
        assert.commandWorked(m.getDB('admin').runCommand({ logApplicationMessage: msg }));

        beforeLoad = Date.now();
        auditColl = getAuditEventsCollection(m, testDBName);
        assert.eq(1, auditColl.count({
            atype: "applicationMessage",
            ts: withinFewSecondsBefore(beforeLoad),
            'param.msg': msg,
            result: 0,
        }), "FAILED, audit log: " + tojson(auditColl.find().toArray()));
    },
    { }
);
