
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function StageExecutor(stageAttempt, executor) {
  this.appId = stageAttempt.appId;
  this.stageId = stageAttempt.stageId;
  this.stageAttemptId = stageAttempt.id;
  this.execId = executor.id;

  this.init([ 'appId', 'stageId', 'stageAttemptId', 'execId' ]);

}

mixinMongoMethods(StageExecutor, 'StageExecutor', 'StageExecutors');

module.exports.StageExecutor = StageExecutor;
