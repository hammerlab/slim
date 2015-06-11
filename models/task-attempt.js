
var mixinMongoMethods = require("../utils").mixinMongoMethods;

function TaskAttempt(appId, stageId, stageAttemptId, id, taskIndex, taskAttemptId) {
  this.appId = appId;
  this.stageId = stageId;
  this.stageAttemptId = stageAttemptId;
  this.id = id;
  this.taskIndex = taskIndex;
  this.taskAttemptId = taskAttemptId;

  this.findObj = { appId: appId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.set({
    stageId: stageId,
    stageAttemptId: stageAttemptId,
    index: taskIndex,
    attempt: taskAttemptId
  });
  this.dirty = true;

}

TaskAttempt.prototype.fromTaskInfo = function(ti) {
  this.set({
    'time.start': this.processTime(ti['Launch Time']),
    'time.end': this.processTime(ti['Finish Time']),
    execId: ti['Executor ID'],
    //host: ti['Host'],  // redundant with exec ID...
    locality: ti['Locality'],
    speculative: ti['Speculative'],
    gettingResultTime: this.processTime(ti['Getting Result Time']),
    failed: ti['Failed'],
    accumulables: ti['Accumulables']
  });
};

mixinMongoMethods(TaskAttempt, "TaskAttempt", "TaskAttempts");

module.exports.TaskAttempt = TaskAttempt;
