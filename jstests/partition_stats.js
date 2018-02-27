t = db[TestData.testName];

// test if one thread has transactionally
// added or dropped a partition, no other
// thread can simultaneously do a fileop
t.drop();

assert.commandWorked(db.runCommand({ create: TestData.testName, partitioned:1}));
assert.commandWorked(db.runCommand({addPartition:TestData.testName, newMax:{_id:10} }));
assert.commandWorked(db.runCommand({addPartition:TestData.testName, newMax:{_id:20} }));
assert.commandWorked(db.runCommand({addPartition:TestData.testName, newMax:{_id:30} }));
assert.commandWorked(db.runCommand({addPartition:TestData.testName, newMax:{_id:40} }));

var st = t.stats({indexDetails:true});
printjson(st);
assert(st.partitioned);
assert.eq(st.nindexes, 1);
assert.eq(st.partitions.length, 5);

for (var i = 0; i < st.indexDetails._id_.partitions.length; ++i) {
    assert.eq(st.indexDetails._id_.partitions[i].PerconaFT.createOptions.compression, 'zlib');
}

// unlike TokuMX reIndex does not accept any arguments
// so following code is useless

//assert.commandWorked(t.reIndex('*', {compression: 'quicklz'}));
//
//var st = t.stats();
//assert(st.partitioned);
//assert.eq(st.nindexes, 1);
//assert.eq(st.partitions.length, 5);
//for (var i = 0; i < st.partitions.length; ++i) {
//    assert.eq(st.indexDetails._id_.partitions[i].PerconaFT.createOptions.compression, 'quicklz');
//}
//
//