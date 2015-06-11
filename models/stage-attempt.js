
var mixinMongoMethods = require("../utils").mixinMongoMethods;

function StageAttempt(appId, stageId, id) {
  this.appId = appId;
  this.stageId = stageId;
  this.id = id;
  this.dirty = true;
  this.findObj = { appId: appId, stageId: this.stageId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};

  this.set({ tasks: {} });
}

StageAttempt.prototype.fromStageInfo = function(si) {
  return this.set({
    'time.start': this.processTime(si['Submission Time']),
    'time.end': this.processTime(si['Completion Time']),
    'taskCounts.num': si['Number of Tasks'],
    failureReason: si['Failure Reason']
  });
};

mixinMongoMethods(StageAttempt, "StageAttempt", "StageAttempts");

module.exports.StageAttempt = StageAttempt;
