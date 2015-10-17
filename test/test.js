
var assert = require("assert");
var chai = require('chai');
var chaiSubset = require('chai-subset');
chai.use(chaiSubset);
var expect = chai.expect;
var fs = require('fs');

var mongo = require("../mongo/collections");
var colls = mongo.colls;
var collections = mongo.collections;

var Server = require('./lib/server').Server;

var utils = require('./lib/utils');
var sortObjs = utils.sortObjs;

function verifyMongoDir(dir, findObjs) {
  var inputDir = dir + "/input";
  var eventLogFile = inputDir + "/events.json";
  var outputDir = dir + "/output";

  describe(dir, function() {
    before('populate mongo', function(done) {
      new Server("mongodb://localhost:27017/test", eventLogFile, done);
    });

    colls.forEach(function(c) {
      var collName = c[0];
      it(
            dir + " should have the expected " + collName + " records",
            function(done) {
              var coll = collections[collName];
              var filename = outputDir + '/' + coll.collectionName + '.json';
              var eObjs = JSON.parse(fs.readFileSync(filename));
              var sortObj = sortObjs[collName];
              var findObj = {};
              if (findObjs && collName in findObjs) {
                console.log("found findObj for:", collName);
                findObj = findObjs[collName];
              }
              collections[collName].find(findObj).sort(sortObj).toArray(function (err, aObjs) {
                assert.equal(null, err);
                assert.equal(eObjs.length, aObjs.length, "Expected " + eObjs.length + " records, got " + aObjs.length);
                eObjs.forEach(function (eObj, idx) {
                  var aObj = aObjs[idx];
                  expect(aObj).to.containSubset(eObj);
                });
                done();
              });
            }
      );

    });
  });
}

verifyMongoDir('test/data/small');
verifyMongoDir('test/data/medium');
verifyMongoDir('test/data/metrics');
verifyMongoDir('test/data/skips');
