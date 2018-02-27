(function() {
    'use strict';

    var conn = MongoRunner.runMongod({});
    var db = conn.getDB('test');

    // use setParameter to set rate limit and getParameter to check current state
    {
        var res = db.getSiblingDB('admin').runCommand( { setParameter: 1, "profilingRateLimit": 79 } );
        assert.commandWorked(res, "setParameter failed to set profilingRateLimit");
        assert.eq(res.was, 1);
        res = db.getSiblingDB('admin').runCommand( { getParameter: 1, "profilingRateLimit": 1 } );
        assert.commandWorked(res, "getParameter failed to get profilingRateLimit");
        assert.eq(res.profilingRateLimit, 79);
    }

    MongoRunner.stopMongod(conn);
})();
