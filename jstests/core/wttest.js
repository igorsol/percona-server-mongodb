// Cannot implicitly shard accessed collections because of extra shard key index in sharded
// collection.
// @tags: [assumes_no_implicit_index_creation]

t = db.bigkeysidxtest;

var keys = [];

var str = "aaaabbbbccccddddeeeeffffgggghhhh";
//var str = "abcdefgh";

while (str.length < 20000) {
    keys.push(str);
    str = str + str;
}

function doInsert(order) {
    if (order == 1) {
        for (var i = 0; i < 10; i++) {
            t.insert({_id: i, k: keys[i]});
        }
    } else {
        for (var i = 9; i >= 0; i--) {
            t.insert({_id: i, k: keys[i]});
        }
    }
}

var expect = null;

function check(cnt) {
    assert(t.validate().valid);
    assert.eq(cnt, t.count());

    var c = t.find({k: /^a/}).count();
    assert.eq(cnt, c);
}

function runTest(order) {
    t.drop();
    t.ensureIndex({k: 1});
    doInsert(order);
    check(5);  // check incremental addition

    //t.reIndex();
    t.dropIndex({k: 1});
    t.ensureIndex({k: 1});
    check(5);  // check bottom up

    t.drop();
    doInsert(order);
    assert.eq(1, t.getIndexes().length);
    t.ensureIndex({k: 1});
    assert.eq(1, t.getIndexes().length);

    t.drop();
    doInsert(order);
    assert.eq(1, t.getIndexes().length);
    t.ensureIndex({k: 1}, {background: true});
    assert.eq(1, t.getIndexes().length);
}

runTest(1);
runTest(2);
