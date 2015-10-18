
var argv = require('minimist')(process.argv.slice(2));

var l = require("../utils/log").l;
var moment = require('moment');

var StageExecutor = require('./stage-executor').StageExecutor;
var Task = require('./task').Task;
var TaskAttempt = require('./task-attempt').TaskAttempt;
var StageSummaryMetric = require('./stage-summary-metric').StageSummaryMetric;
var utils= require("../utils/utils");
var processTime = utils.processTime;
var accumulablesObj = utils.accumulablesObj;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

var getExecutorId = require('./executor').getExecutorId;

var metricIds = require('./task-attempt').metricIds;

function StageAttempt(stage, id) {
  this.app = stage.app;
  this.appId = stage.appId;
  this.stageId = stage.id;
  this.id = id;

  if (!stage.job) {
    l.error("%s: stage missing job (stage %s)", this.toString(), stage.toString());
  } else {
    this.job = stage.job;
  }

  this.tasks = {};
  this.task_attempts = {};

  this.executors = {};

  this.metrics = metricIds.map(function(id) {
    return new StageSummaryMetric(this, id);
  }.bind(this));

  var callbackObjs = [ this.app ].concat(this.job ? [ this.job ] : []);
  this.metricsMap = {};
  var callbackObj = {
    failing: {
      sums: callbackObjs
    },
    failed: {
      sums: callbackObjs
    },
    taskCounts: {
      sums: callbackObjs
    },
    taskIdxCounts: {
      sums: callbackObjs
    }
  };
  this.metrics.forEach(function(metric) {
    this.metricsMap[metric.id] = metric;
  }.bind(this));

  this.init(
        [ 'appId', 'stageId', 'id' ],
        callbackObj
  );

  if (this.job) this.set('jobId', this.job.id);

  this.commitHooks.push(this.syncSummaryMetrics);

}

StageAttempt.prototype.initMetrics = function() {
  this.metrics.forEach(function(metric) {
    metric.initTree();
  });
};

StageAttempt.prototype.fromStageInfo = function(si) {
  return this
        .set({
          'time.start': si['Submission Time'],
          'accumulables': accumulablesObj(si['Accumulables'])
        }, true)
        .set({
          name: si['Stage Name'],
          'time.end': processTime(si['Completion Time']),
          'taskCounts.num': si['Number of Tasks'],
          'taskIdxCounts.num': si['Number of Tasks'],
          failureReason: si['Failure Reason']
        });
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
    if (!(taskId in this.app.task_attempts)) {
      var task = new TaskAttempt(this, taskId);
      this.task_attempts[taskId] = task;
      this.app.task_attempts[taskId] = task;
    } else {
      var task = this.app.task_attempts[taskId];
      l.warn(
            "In app %s: looking for task %d from stage-attempt %d.%d in " +
            "stage-attempt %d.%d; this is likely due to a Spark bug (see " +
            "SPARK-9366) where the latest attempt for a stage is used in " +
            "the TaskEnd event, instead of the attempt that the task " +
            "actually belongs to.",
            this.appId,
            taskId,
            task.stageId, task.stageAttemptId,
            this.stageId, this.id
      );
      return task;
    }
  }
  return this.task_attempts[taskId];
};

StageAttempt.prototype.getExecutor = function(executor) {
  var execId = executor.id;
  if (!(execId in this.executors)) {
    this.executors[execId] = new StageExecutor(this, executor);
  }
  return this.executors[execId];
};

StageAttempt.prototype.syncSummaryMetrics = function() {
  for (var metricId in this.metrics) {
    this.metrics[metricId].syncChanges();
  }
};

mixinMongoMethods(StageAttempt, "StageAttempt", "StageAttempts");

module.exports.StageAttempt = StageAttempt;
