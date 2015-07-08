
var assert = require('assert');

var MongoClient = require('mongodb').MongoClient;

var l = require('../utils/log').l;

module.exports = {
  // Mongo collection placeholders.
  Applications: null,
  Jobs: null,
  Stages: null,
  StageAttempts: null,
  RDDs: null,
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
    module.exports.Jobs = db.collection('jobs');
    module.exports.Stages = db.collection('stages');
    module.exports.StageAttempts = db.collection('stage_attempts');
    module.exports.RDDs = db.collection('rdds');
    module.exports.Executors = db.collection('executors');
    module.exports.Tasks = db.collection('tasks');
    module.exports.TaskAttempts = db.collection('task_attempts');
    module.exports.Environment = db.collection('environment');

    collNamesAndIndices = [
      ['Applications', { id: 1 } ],
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

    collNamesAndIndices.forEach(function(collNameAndIndex) {
      var name = collNameAndIndex[0];
      var fields = collNameAndIndex[1];
      module.exports[name].ensureIndex(fields, function(err) {
        if (err) {
          l.error("Error creating index %O for collection %s: %O", fields, name, err);
        } else {
          l.warn("Created index %O for collection %s", fields, name);
        }
      });
    });

    cb(db);

  });
};

