
var assert = require('assert');
var async = require('async');

var MongoClient = require('mongodb').MongoClient;

var l = require('../utils/log').l;

var colls = module.exports.colls = [
  [ 'Applications', 'apps', [{ id: 1 }] ],
  [ 'Jobs', 'jobs', [{ appId: 1, id: 1 }] ],
  [ 'Stages', 'stages', [{ appId: 1, id: 1 }, { appId: 1, jobId: 1 }] ],
  [ 'Graphs', 'graphs', [{ appId: 1, stageId: 1 }, { appId: 1, stageId: 1 }] ],
  [ 'StageAttempts', 'stage_attempts', [{ appId: 1, stageId: 1, id: 1 }] ],
  [ 'StageExecutors', 'stage_executors', [{ appId: 1, stageId: 1, execId: 1 }] ],
  [ 'RDDs', 'rdds', [{ appId: 1, id: 1 }] ],
  [ 'RDDExecutors', 'rdd_executors', [{ appId: 1, rddId: 1, execId: 1 }] ],
  [ 'NonRddBlocks', 'non_rdd_blocks', [{ appId: 1, execId: 1, id: 1 }] ],
  [ 'RddBlocks', 'rdd_blocks', [{ appId: 1, rddId: 1, id: 1 }] ],
  [ 'Executors', 'executors', [{ appId: 1, id: 1 }] ],
  [ 'Tasks', 'tasks', [{ appId: 1, stageId: 1, id: 1 }] ],
  [
    'TaskAttempts',
    'task_attempts',
    [
      'id',
      'metrics.JVMGCTime',
      'metrics.ShuffleReadMetrics.TotalBytesRead',
      'metrics.ShuffleReadMetrics.TotalRecordsRead',
      'metrics.ShuffleWriteMetrics.ShuffleBytesWritten',
      'metrics.ShuffleWriteMetrics.ShuffleRecordsWritten',
      'metrics.InputMetrics.BytesRead',
      'metrics.InputMetrics.RecordsRead',
      'metrics.OutputMetrics.BytesWritten',
      'metrics.OutputMetrics.RecordsWritten',
      'metrics.ExecutorRunTime.Bytes',
      'time.start',
      'metrics.HostName',
      'status',
      'index',
      'execId',
      'time.end',
      'metrics.MemoryBytesSpilled',
      'metrics.DiskBytesSpilled',
      'metrics.ResultSize',
      'metrics.GettingResultTime',
      'metrics.SchedulerDelayTime',
      'duration'
    ].map(function(key) {
            var o = {appId: 1, stageId: 1, stageAttemptId: 1};
            o[key] = 1;
            return o;
          }
    )
  ],
  [ 'StageSummaryMetrics', 'stage_summary_metrics', [{ appId: 1, stageId: 1, stageAttemptId: 1, id: 1 }] ],
  [ 'Environment', 'environment', [{ appId: 1 }] ]
];

var collections = module.exports.collections = {};
var collectionsArr = module.exports.collectionsArr = [];

var collNamesAndIndices = [];
colls.forEach(function(coll) {
  collections[coll[0]] = null;
  coll[2].forEach(function(index) {
    collNamesAndIndices.push({ collName: coll[0], index: index });
  });
}.bind(this));

var db = null;
function dropDatabase(cb) {
  db.dropDatabase(cb);
}

function initColls() {
  colls.forEach(function(coll) {
    collections[coll[0]] = db.collection(coll[1]);
    collectionsArr.push(collections[coll[0]]);
  }.bind(this));
}

function ensureIndexes(cb) {
  l.info("Creating indexes...");
  async.parallel(
        collNamesAndIndices.map(function(o) {
          return function(callback) {
            collections[o.collName].ensureIndex(o.index, callback);
          }.bind(this);
        }.bind(this)),
        function(err) {
          if (err) {
            l.error("Failed to create Mongo indexes");
          } else {
            l.info("Done creating indexes");
          }

          cb();
        }
  );
}

module.exports.init = function(url, cb) {
  var isTest = !!url.match(/\/test$/);
  if (isTest) {
    module.exports.dropDatabase = dropDatabase;
  }
  l.info("Connecting to Mongo:", url);
  MongoClient.connect(url, function(err, d) {
    assert.equal(null, err);
    l.warn("Connected to Mongo");
    db = d;
    initColls();
    ensureIndexes(cb);
  });
};
