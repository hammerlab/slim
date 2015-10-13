
var argv = require('minimist')(process.argv.slice(2));

var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function Task(stageAttempt, id) {
  this.appId = stageAttempt.appId;
  this.stageId = stageAttempt.stageId;
  this.stageAttemptId = stageAttempt.id;
  this.id = id;

  this.init([ 'appId', 'stageId', 'stageAttemptId', 'id' ]);
}

mixinMongoMethods(Task, "Task", "Tasks");

Task.lowPriority = true;
module.exports.Task = Task;
