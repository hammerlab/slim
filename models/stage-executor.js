
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function StageExecutor(stageAttempt, executorId) {
  this.appId = stageAttempt.appId;
  this.stageId = stageAttempt.stageId;
  this.stageAttemptId = stageAttempt.id;
  this.execId = executorId;

  this.init([ 'appId', 'stageId', 'stageAttemptId', 'execId' ]);

}

mixinMongoMethods(StageExecutor, 'StageExecutor', 'StageExecutors');

module.exports.StageExecutor = StageExecutor;
