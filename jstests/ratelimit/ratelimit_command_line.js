(function() {
    'use strict';

    // set rateLimit 100 on the command line
    var conn = MongoRunner.runMongod({rateLimit: 100});
    var db = conn.getDB('test');

    // compare reported rateLimit with the value set on the command line
    {
        var ps = db.getProfilingStatus();
        assert.eq(ps.was, 0);
        assert.eq(ps.slowms, 100);
        assert.eq(ps.ratelimit, 100);
    }

    MongoRunner.stopMongod(conn);
})();
