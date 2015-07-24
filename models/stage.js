
var removeKeyDots = require("../utils/objs").removeKeyDots;
var utils = require("../utils/utils");

var StageAttempt = require('./stage-attempt').StageAttempt;
var Task = require('./task').Task;

var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

var RUNNING = utils.RUNNING;
var FAILED = utils.FAILED;
var SUCCEEDED = utils.SUCCEEDED;

function Stage(app, id) {
  this.app = app;
  this.appId = app.id;
  this.id = id;

  this.init([ 'appId', 'id' ]);

  this.attempts = {};
}

mixinMongoMethods(Stage, "Stage", "Stages");

Stage.prototype.fromStageInfo = function(si) {
  return this.set({
    name: si['Stage Name'],
    rddIDs: si['RDD Info'].map(function (ri) {
      return ri['RDD ID'];
    }),
    parents: si['Parent IDs'],
    details: si['Details']
  }).set({
    'accumulables': removeKeyDots(si['Accumulables']),
    'taskCounts.num': si['Number of Tasks']
  }, true);
};

Stage.prototype.getAttempt = function(attemptId) {
  if (typeof attemptId == 'object') {
    attemptId = attemptId['Stage Attempt ID'];
  }
  if (!(attemptId in this.attempts)) {
    this.attempts[attemptId] = new StageAttempt(this, attemptId);
  }
  return this.attempts[attemptId];
};

Stage.prototype.setJob = function(job) {
  this.job = job;
  this.set('jobId', job.id);
  return this;
};

module.exports.Stage = Stage;
