
var assert = require('assert');
var async = require('async');

var MongoClient = require('mongodb').MongoClient;

var l = require('../utils/log').l;

module.exports = {
  // Mongo collection placeholders.
  Applications: null,
  Jobs: null,
  Stages: null,
  StageAttempts: null,
  RDDs: null,
  NonRddBlocks: null,
  RddBlocks: null,
  Executors: null,
  Tasks: null,
  TaskAttempts: null,
  Environment: null
};


module.exports.init = function(url, cb) {
  MongoClient.connect(url, function(err, db) {
    assert.equal(null, err);
    l.warn("Connected to Mongo");

    module.exports.Applications = db.collection('apps');
    module.exports.RddBlocks = db.collection('rdd_blocks');
    module.exports.NonRddBlocks = db.collection('non_rdd_blocks');
    module.exports.Jobs = db.collection('jobs');
    module.exports.Stages = db.collection('stages');
    module.exports.StageAttempts = db.collection('stage_attempts');
    module.exports.RDDs = db.collection('rdds');
    module.exports.Executors = db.collection('executors');
    module.exports.Tasks = db.collection('tasks');
    module.exports.TaskAttempts = db.collection('task_attempts');
    module.exports.Environment = db.collection('environment');

    collNamesAndIndices = [
      [ 'Applications', { id: 1 } ],
      [ 'RddBlocks', { appId: 1, rddId: 1, id: 1 } ],
      [ 'NonRddBlocks', { appId: 1, execId: 1, id: 1 } ],
      [ 'Jobs', { appId: 1, id: 1 } ],
      [ 'Stages', { appId: 1, id: 1 } ],
      [ 'Stages', { appId: 1, jobId: 1 } ],
      [ 'StageAttempts', { appId: 1, stageId: 1, id: 1 } ],
      [ 'RDDs', { appId: 1, id: 1 } ],
      [ 'Executors', { appId: 1, id: 1 } ],
      [ 'Tasks', { appId: 1, stageId: 1, id: 1 } ],
      [ 'TaskAttempts', { appId: 1, stageId: 1, stageAttemptId: 1, id: 1 } ],
      [ 'Environment', { appId: 1 } ]
    ];

    async.parallel(
          collNamesAndIndices.map(function(collNameAndIndex) {
            return function(callback) {
              var name = collNameAndIndex[0];
              var fields = collNameAndIndex[1];
              module.exports[name].ensureIndex(fields, callback);
            };
          }),
          function(err) {
            if (err) {
              l.error("Error creating mongo indexes:", JSON.stringify(err));
            } else {
              l.info("Mongo indexes created successfully.");
            }
            cb(db);
          }
    );
  });
};

