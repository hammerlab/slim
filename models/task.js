
var argv = require('minimist')(process.argv.slice(2));

var l = require("../utils/log").l;

var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function Task(stageAttempt, id) {
  this.appId = stageAttempt.appId;
  this.stageId = stageAttempt.stageId;
  this.stageAttemptId = stageAttempt.id;
  this.id = id;

  this.init([ 'appId', 'stageId', 'stageAttemptId', 'id' ]);
}

mixinMongoMethods(Task, "Task", "Tasks", -2);

if (argv.nt || argv['no-task-upserts']) {
  l.info("Disabling upserts for Tasks");
  Task.noUpsert = true;
}

module.exports.Task = Task;
