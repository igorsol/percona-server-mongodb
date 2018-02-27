(function() {
    'use strict';

    var conn = MongoRunner.runMongod({});
    var db = conn.getDB('test');

    // use runCommand(profile) to set rateLimit and to check current state
    {
        var res = db.runCommand( { profile: 2, slowms: 150, ratelimit: 3 } );
        assert.commandWorked(res, "'profile' command failed to set profiling options");
        assert.eq(res.was, 0);
        assert.eq(res.slowms, 100);
        assert.eq(res.ratelimit, 1);
        res = db.runCommand( { profile: -1 } );
        assert.commandWorked(res, "'profile' command failed to get current configuration");
        assert.eq(res.was, 2);
        assert.eq(res.slowms, 150);
        assert.eq(res.ratelimit, 3);
    }

    MongoRunner.stopMongod(conn);
})();
