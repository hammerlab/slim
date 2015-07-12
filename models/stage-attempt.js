
var TaskAttempt = require('./task-attempt').TaskAttempt;

var removeKeyDots = require("../utils/objs").removeKeyDots;
var processTime = require("../utils/utils").processTime;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

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


  this.tasks = {};
  this.task_attempts = {};
}

StageAttempt.prototype.fromStageInfo = function(si) {
  return this.set({
    name: si['Stage Name'],
    'time.start': processTime(si['Submission Time']),
    'time.end': processTime(si['Completion Time']),
    'taskCounts.num': si['Number of Tasks'],
    'taskIdxCounts.num': si['Number of Tasks'],
    failureReason: si['Failure Reason']
  }).set('accumulables', removeKeyDots(si['Accumulables']), true).setDuration();
};

StageAttempt.prototype.getTask = function(taskIndex) {
  if (typeof taskIndex == 'object') {
    taskIndex = taskIndex['Index'];
  }
  if (!(taskIndex in this.tasks)) {
    this.tasks[taskIndex] = new Task(this.appId, this, taskIndex);
  }
  return this.tasks[taskIndex];
};

StageAttempt.prototype.getTaskAttempt = function(taskId) {
  if (typeof taskId == 'object') {
    taskId = taskId['Task ID'];
  }
  if (!(taskId in this.task_attempts)) {
    this.task_attempts[taskId] = new TaskAttempt(this.appId, this, taskId);
  }
  return this.task_attempts[taskId];
};


mixinMongoMethods(StageAttempt, "StageAttempt", "StageAttempts");

module.exports.StageAttempt = StageAttempt;
