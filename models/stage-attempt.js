
var moment = require('moment');

var StageExecutor = require('./stage-executor').StageExecutor;
var Task = require('./task').Task;
var TaskAttempt = require('./task-attempt').TaskAttempt;

var removeKeyDots = require("../utils/objs").removeKeyDots;
var processTime = require("../utils/utils").processTime;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

var getExecutorId = require('./executor').getExecutorId;

function StageAttempt(stage, id) {
  this.app = stage.app;
  this.appId = stage.appId;
  this.stageId = stage.id;
  this.id = id;

  this.init([ 'appId', 'stageId', 'id' ]);

  this.tasks = {};
  this.task_attempts = {};

  this.executors = {};
}

StageAttempt.prototype.setJob = function(job) {
  this.job = job;
  this.set('jobId', job.id);
  return this;
};

StageAttempt.prototype.fromStageInfo = function(si) {
  return this
        .set('time.start', si['Submission Time'] || (moment().unix() * 1000), true)
        .set({
          name: si['Stage Name'],
          'time.end': processTime(si['Completion Time']),
          'taskCounts.num': si['Number of Tasks'],
          'taskIdxCounts.num': si['Number of Tasks'],
          failureReason: si['Failure Reason']
        })
        .set('accumulables', removeKeyDots(si['Accumulables']), true)
        .setDuration();
};

StageAttempt.prototype.getTask = function(taskIndex) {
  if (typeof taskIndex == 'object') {
    taskIndex = taskIndex['Index'];
  }
  if (!(taskIndex in this.tasks)) {
    this.tasks[taskIndex] = new Task(this, taskIndex);
  }
  return this.tasks[taskIndex];
};

StageAttempt.prototype.getTaskAttempt = function(taskId) {
  if (typeof taskId == 'object') {
    taskId = taskId['Task ID'];
  }
  if (!(taskId in this.task_attempts)) {
    this.task_attempts[taskId] = new TaskAttempt(this, taskId);
  }
  return this.task_attempts[taskId];
};

StageAttempt.prototype.getExecutor = function(execId) {
  execId = getExecutorId(execId);
  if (!(execId in this.executors)) {
    this.executors[execId] = new StageExecutor(this, execId);
  }
  return this.executors[execId];
};

mixinMongoMethods(StageAttempt, "StageAttempt", "StageAttempts");

module.exports.StageAttempt = StageAttempt;
