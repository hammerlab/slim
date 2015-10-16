
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function StageExecutor(stageAttempt, executor) {
  this.appId = stageAttempt.appId;
  this.stageId = stageAttempt.stageId;
  this.stageAttemptId = stageAttempt.id;
  this.execId = executor.id;

  var stage = stageAttempt.stage;

  var taskCountCallbackObjs =
        [ stageAttempt, executor, stage, stage.app ]
              .concat(stage.job ? [ stage.job ] : []);

  this.init(
        [ 'appId', 'stageId', 'stageAttemptId', 'execId' ],
        {
          taskCounts: {
            num: { sums: [ executor ] },
            running: { sums: taskCountCallbackObjs },
            succeeded: { sums: taskCountCallbackObjs },
            failed: { sums: taskCountCallbackObjs },
            skipped: { sums: taskCountCallbackObjs }
          }
        }
  );

}

mixinMongoMethods(StageExecutor, 'StageExecutor', 'StageExecutors');

module.exports.StageExecutor = StageExecutor;
