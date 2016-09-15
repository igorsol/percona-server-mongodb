// check default values
{
	var ps = db.getProfilingStatus();
	assert.eq(ps.was, 0);
	assert.eq(ps.slowms, 100);
	assert.eq(ps.ratelimit, 1);
}

// setParameter/getParameter
{
	var res = db.getSiblingDB('admin').runCommand( { setParameter: 1, "profilingRateLimit": 79 } );
	assert.commandWorked(res, "setParameter failed to set profilingRateLimit");
	assert.eq(res.was, 1);
	res = db.getSiblingDB('admin').runCommand( { getParameter: 1, "profilingRateLimit": 1 } );
	assert.commandWorked(res, "getParameter failed to get profilingRateLimit");
	assert.eq(res.profilingRateLimit, 79);
}

// runCommand(profile)
{
	var res = db.runCommand( { profile: 2, slowms: 150, ratelimit: 3 } );
	assert.commandWorked(res, "'profile' command failed to set profiling options");
	assert.eq(res.was, 0);
	assert.eq(res.slowms, 100);
	assert.eq(res.ratelimit, 79);
	res = db.runCommand( { profile: -1 } );
	assert.commandWorked(res, "'profile' command failed to get current configuration");
	assert.eq(res.was, 2);
	assert.eq(res.slowms, 150);
	assert.eq(res.ratelimit, 3);
}

// now we have rateLimit set to 3
// let's check real distribution
for (i = 0; i < 1000; i++) {
    db.batch.insert( { n: i, tag: "rateLimit test" } );
}

cnt = db.system.profile.count( { op: "insert", ns: "test.batch", "query.documents.0.tag": "rateLimit test" } );
assert(300 < cnt && cnt < 366, "rateLimit failed to filter events (" + cnt + ")");

// check that all profiled events have correct 'rateLimit' value
cnt2 = db.system.profile.count( { op: "insert", ns: "test.batch", "query.documents.0.tag": "rateLimit test" , rateLimit: 3} );
assert.eq(cnt, cnt2);

// put some slow events and check their behavior
for (i = 0; i < 10; i++) {
    db.batch.count( { $where: "(this.n == 0) ? (sleep(200) || true) : false" } );
}

// ensure that rateLimit is 1 for slow queries despite the fact current rateLimit is set to 3
// also ensure that all slow queries are profiled
cnt3 = db.system.profile.count( { op: "command", ns: "test.batch" , millis: {$gte: 200}, rateLimit: 1} );
if (cnt3 != 10) {
	db.system.profile.find( { op: "command", ns: "test.batch"} ).forEach( printjsononeline );
}
assert.eq(cnt3, 10);
