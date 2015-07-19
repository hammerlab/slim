
var argv = require('minimist')(process.argv.slice(2));
var assert = require('assert');
var async = require('async');

var mongoPort = argv.p || argv['mongo-port'] || 27017;
var mongoHost = argv.h || argv['mongo-host'] || 'localhost';
var mongoDb = argv.d || argv['mongo-db'] || 'test';
var mongoUrl = argv.m || argv['mongo-url'] || ('mongodb://' + mongoHost + ':' + mongoPort + '/' + mongoDb);

var mongo = require('../../mongo/collections');
var colls = mongo.colls;
var collections = mongo.collections;
var collectionsArr = mongo.collectionsArr;

if (argv._.length !== 1) {
  throw new Error("Usage: node dump-records.js [opts] <dir>");
}
var dir = argv._[0];

var fs = require('fs');
var mkdirp = require('mkdirp');
mkdirp.sync(dir);

var sortObjs = require('./sort-objs').sortObjs;

mongo.init(mongoUrl, function(err) {
  assert.equal(null, err);
  async.parallel(
        colls.map(function(c) {
          var name = c[0];
          var coll = collections[name];
          var sortObj = sortObjs[name];
          return function(callback) {
            coll.find({}, { _id: 0, l: 0, n: 0 }).sort(sortObj).toArray(function(err, objs) {
              assert.equal(null, err);
              var filename = dir + '/' + coll.collectionName + '.json';
              console.log("Writing:", filename);
              fs.writeFileSync(filename, JSON.stringify(objs, null, 2));
              callback();
            });
          };
        }),
        function(err) {
          assert.equal(null, err);
          console.log("Success!");
        }
  );
});
