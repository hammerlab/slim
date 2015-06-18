
var mixinMongoMethods = require("../utils").mixinMongoMethods;

function TaskAttempt(appId, stageId, stageAttemptId, id) {
  this.appId = appId;
  this.stageId = stageId;
  this.stageAttemptId = stageAttemptId;
  this.id = id;

  this.findObj = { appId: appId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.set({
    stageId: stageId,
    stageAttemptId: stageAttemptId
  });
  this.dirty = true;
  this.key = [ 'app', appId, 'task', id ].join('-');

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
    accumulables: ti['Accumulables'],
    index: ti['Index'],
    attempt: ti['Attempt']
  });
};

mixinMongoMethods(TaskAttempt, "TaskAttempt", "TaskAttempts");

module.exports.TaskAttempt = TaskAttempt;
