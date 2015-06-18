
var mixinMongoMethods = require("../utils").mixinMongoMethods;

function Task(appId, stageId, id) {
  this.appId = appId;
  this.stageId = stageId;
  this.id = id;

  this.findObj = { appId: appId, stageId: stageId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;
  this.key = [ 'app', appId, 'stage', stageId, 'task', id ].join('-');

  this.set({
    attempts: {},
    numAttempts: 0,
    'attemptCounts.running': 0,
    'attemptCounts.failed': 0,
    'attemptCounts.succeeded': 0
  });

}

mixinMongoMethods(Task, "Task", "Tasks");

module.exports.Task = Task;
