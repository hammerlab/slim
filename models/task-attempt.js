
var mixinMongoMethods = require("../utils").mixinMongoMethods;

function TaskAttempt(appId, id) {
  this.appId = appId;
  this.id = id;

  this.findObj = { appId: appId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
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
