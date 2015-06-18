
var mixinMongoMethods = require("../utils").mixinMongoMethods;

function Job(appId, id) {
  this.appId = appId;
  this.id = id;
  this.findObj = { appId: appId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;
  this.key = [ 'app', appId, 'job', id ].join('-');
}

mixinMongoMethods(Job, "Job", "Jobs");

module.exports.Job = Job;
