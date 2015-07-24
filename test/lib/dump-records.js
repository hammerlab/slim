
var argv = require('minimist')(process.argv.slice(2));
var assert = require('assert');
var async = require('async');

var mongoPort = argv.p || argv['mongo-port'] || 27017;
var mongoHost = argv.h || argv['mongo-host'] || 'localhost';
var mongoDb = argv.d || argv['mongo-db'] || 'test';
var mongoUrl = argv.m || argv['mongo-url'] || ('mongodb://' + mongoHost + ':' + mongoPort + '/' + mongoDb);

var inputFile = argv.in;

var mongo = require('../../mongo/collections');
var colls = mongo.colls;
var collections = mongo.collections;
var collectionsArr = mongo.collectionsArr;

var Server = require('./server').Server;

if (argv._.length !== 1 && !inputFile) {
  throw new Error(
        "Specify either an argument (where to write JSON files) or an --in option (where to read from; output paths automatically inferred in this case)"
  );
}
var outputDir = null;
if (argv._.length) {
  outputDir = argv._[0];
}

var fs = require('fs');
var mkdirp = require('mkdirp');
var path = require('path');

var sortObjs = require('./utils').sortObjs;

function dumpMongoToOutputDir(dir) {
  mkdirp.sync(dir);
  async.parallel(
        colls.map(function (c) {
          var name = c[0];
          var coll = collections[name];
          var sortObj = sortObjs[name];
          return function (callback) {
            coll.find({}, {_id: 0, l: 0, n: 0}).sort(sortObj).toArray(function (err, objs) {
              assert.equal(null, err);
              var filename = dir + '/' + coll.collectionName + '.json';
              console.log("Writing:", filename);
              fs.writeFile(filename, JSON.stringify(objs, null, 2), callback);
            });
          };
        }),
        function (err) {
          assert.equal(null, err);
          console.log("Success!");
          process.exit(0);
        }
  );
}

if (inputFile) {
  if (!outputDir) {
    var inputDir = null;
    var rootDir = null;
    var stat = fs.statSync(inputFile);
    if (stat.isDirectory()) {
      var basename = path.basename(inputFile);
      if (basename === 'input') {
        inputDir = inputFile;
        inputFile = path.join(inputFile, 'events.json');
        rootDir = path.dirname(inputDir);
        if (!fs.existsSync(inputFile)) {
          throw new Error("Couldn't find events.json under " + inputFile);
        }
      } else {
        rootDir = inputFile;
        inputDir = path.join(rootDir, 'input');
        inputFile = path.join(inputDir, 'events.json');
        if (!fs.existsSync(inputFile)) {
          throw new Error("Couldn't find events.json or input/events.json under " + inputFile);
        }
      }
    } else {
      inputDir = path.dirname(inputFile);
      rootDir = path.dirname(inputDir);
    }
    outputDir = path.join(rootDir, 'output');
  }
  new Server(mongoUrl, inputFile, function() {
    dumpMongoToOutputDir(outputDir);
  });
} else {
  mongo.init(mongoUrl, function (err) {
    assert.equal(null, err);
    dumpMongoToOutputDir(outputDir);
  });
}
