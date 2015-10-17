
var assert = require('assert');
var deq = require('deep-equal');

var mixinMongoMethods = require('../mongo/record').mixinMongoMethods;

var Foo = function(id) {
  this.id = id;
  this.enqueued = true;
  this.init(['id']);
};
mixinMongoMethods(Foo, "Foo", "foo");

function makeBar(callbackObj) {
  var Bar = function(foo, id) {
    this.fooId = foo.id;
    this.id = id;

    this.enqueued = true;
    this.init(
          ['fooId', 'id'],
          callbackObj
    );
    this.needsUpsert = true;
  };
  mixinMongoMethods(Bar, "Bar", "bar");
  return Bar;
}


describe("record test", function() {
  it("should update baz", function (done) {
    var foo = new Foo("abc");
    var Bar = makeBar({
      baz: {
        sums: [ foo ]
      }
    });
    var bar = new Bar(foo, "bar");

    bar.set('baz', 4);

    assert.equal(bar.get('baz'), 4);
    assert.equal(foo.get('baz'), 4);
    done();
  });

  it("should update nested fields", function (done) {
    var foo = new Foo("abc");
    var Bar = makeBar({
      "baz.qux": {
        sums: [ foo ]
      }
    });
    var bar = new Bar(foo, "bar");

    bar.set('baz', { qux: 4 });

    assert.equal(bar.get('baz.qux'), 4);
    assert.deepEqual(bar.get('baz'), { qux: 4 });

    assert.equal(foo.get('baz.qux'), 4);
    assert.deepEqual(foo.get('baz'), { qux: 4 });

    done();
  });

  it("should update deeply nested fields", function (done) {
    var foo1 = new Foo("foo1");
    var foo2 = new Foo("foo2");
    var Bar = makeBar({
      "a.b.c": {
        sums: [ foo1 ]
      },
      "b.d.e": {
        sums: [ foo2 ]
      }
    });

    var bar = new Bar(foo1, "bar");

    bar.set('a', { b: { c: 4 } });
    bar.set('b.d.e', 5);

    assert.equal(bar.get('a.b.c'), 4);
    assert.deepEqual(bar.get('a.b'), { c: 4 });
    assert.deepEqual(bar.get('a'), { b: { c: 4 } });

    assert.equal(foo1.get('a.b.c'), 4);
    assert.deepEqual(foo1.get('a.b'), { c: 4 });
    assert.deepEqual(foo1.get('a'), { b: { c: 4 } });

    assert.equal(foo2.get('b.d.e'), 5);
    assert.deepEqual(foo2.get('b.d'), { e: 5 });
    assert.deepEqual(foo2.get('b'), { d: { e: 5 } });

    done();
  });

});
