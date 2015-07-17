
var argv = require('minimist')(process.argv.slice(2));

var l = require("../utils/log").l;
var subRecord = !!argv.s;

var apps = null;
var processTime = require("../utils/utils").processTime;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;
var mixinMongoSubrecordMethods = require("../mongo/subrecord").mixinMongoSubrecordMethods;

function TaskAttempt(stageAttempt, id) {
  this.app = stageAttempt.app;
  this.stageAttempt = stageAttempt;
  this.job = stageAttempt.job;

  this.appId = stageAttempt.appId;
  this.stageId = stageAttempt.stageId;
  this.stageAttemptId = stageAttempt.id;
  this.id = id;

  if (subRecord) {
    this.super = stageAttempt;
    this.superKey = ['tasks', id, ''].join('.');
    this.set('id', id);
  } else {
    this.init(
          [ 'appId', 'stageId', 'stageAttemptId', 'id' ],
          'totalTaskDuration',
          [ this.stageAttempt, this.job, this.app ]
    );
  }
}

var getExecutorId = require('./executor').getExecutorId;

TaskAttempt.prototype.fromTaskInfo = function(ti) {
  this.set({
    'time.start': processTime(ti['Launch Time']),
    'time.end': processTime(ti['Finish Time']),
    execId: getExecutorId(ti),
    host: ti['Host'],
    locality: ti['Locality'],
    speculative: ti['Speculative'],
    gettingResultTime: processTime(ti['Getting Result Time']),
    index: ti['Index'],
    attempt: ti['Attempt']
  }).set('accumulables', ti['Accumulables'], true).setExecutors();
  return this;
};

TaskAttempt.prototype.setExecutors = function() {
  if (!this.executor && this.has('execId')) {
    var execId = this.get('execId');
    this.executor = this.app.getExecutor(execId);
    this.stageExecutor = this.stageAttempt.getExecutor(execId);
    this.durationAggregationObjs.push(this.executor);
    this.durationAggregationObjs.push(this.stageExecutor);
  }
  return this;
};

if (subRecord) {
  mixinMongoSubrecordMethods(TaskAttempt, "TaskAttempt");
} else {
  mixinMongoMethods(TaskAttempt, "TaskAttempt", "TaskAttempts");
}

module.exports.TaskAttempt = TaskAttempt;
