
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function Job(appId, id) {
  this.appId = appId;
  this.id = id;

  this.init([ 'appId', 'id' ]);
}

mixinMongoMethods(Job, "Job", "Jobs");

module.exports.Job = Job;
