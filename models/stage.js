
var utils = require("../utils/utils");

var StageAttempt = require('./stage-attempt').StageAttempt;
var Task = require('./task').Task;

var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

var RUNNING = utils.RUNNING;
var FAILED = utils.FAILED;
var SUCCEEDED = utils.SUCCEEDED;

function Stage(appId, id) {
  this.appId = appId;
  this.id = id;
  this.dirty = true;

  this.findObj = { appId: appId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.key = [ 'app', appId, 'stage', id ].join('-');
  this.applyRateLimit = true;

  this.attempts = {};
  this.tasks = {};
}

mixinMongoMethods(Stage, "Stage", "Stages");

Stage.prototype.fromStageInfo = function(si) {
  return this.set({
    name: si['Stage Name'],
    'taskCounts.num': si['Number of Tasks'],
    rddIDs: si['RDD Info'].map(function (ri) {
      //console.log("rdd id: " + ri["RDD ID"])
      return ri['RDD ID'];
    }),
    parents: si['Parent IDs'],
    details: si['Details'],
    accumulables: si['Accumulables']
  });
};

Stage.prototype.getAttempt = function(attemptId) {
  if (typeof attemptId == 'object') {
    attemptId = attemptId['Stage Attempt ID'];
  }
  if (!(attemptId in this.attempts)) {
    this.attempts[attemptId] = new StageAttempt(this.appId, this.id, attemptId);
  }
  return this.attempts[attemptId];
};

Stage.prototype.getTask = function(taskIndex) {
  if (typeof taskIndex == 'object') {
    taskIndex = taskIndex['Index'];
  }
  if (!(taskIndex in this.tasks)) {
    this.tasks[taskIndex] = new Task(this.appId, this, taskIndex);
  }
  return this.tasks[taskIndex];
};

module.exports.Stage = Stage;
