
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function Job(app, id) {
  this.app = app;
  this.appId = app.id;
  this.id = id;

  this.init([ 'appId', 'id' ], 'totalJobDuration', [app]);
}

mixinMongoMethods(Job, "Job", "Jobs");

module.exports.Job = Job;
