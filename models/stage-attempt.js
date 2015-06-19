
var TaskAttempt = require('./task-attempt').TaskAttempt;

var mixinMongoMethods = require("../utils").mixinMongoMethods;

function StageAttempt(appId, stageId, id) {
  this.appId = appId;
  this.stageId = stageId;
  this.id = id;
  this.dirty = true;
  this.findObj = { appId: appId, stageId: this.stageId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.key = [ 'app', appId, 'stage', stageId, 'attempt', id ].join('-');
  this.applyRateLimit = true;

  this.task_attempts = {};
}

StageAttempt.prototype.fromStageInfo = function(si) {
  return this.set({
    'time.start': this.processTime(si['Submission Time']),
    'time.end': this.processTime(si['Completion Time']),
    'taskCounts.num': si['Number of Tasks'],
    failureReason: si['Failure Reason']
  });
};

StageAttempt.prototype.getTaskAttempt = function(taskId) {
  if (typeof taskId == 'object') {
    taskId = taskId['Task ID'];
  }
  if (!(taskId in this.task_attempts)) {
    this.task_attempts[taskId] = new TaskAttempt(this.appId, this.stageId, this.id, taskId);
  }
  return this.task_attempts[taskId];
};


mixinMongoMethods(StageAttempt, "StageAttempt", "StageAttempts");

module.exports.StageAttempt = StageAttempt;
