
var argv = require('minimist')(process.argv.slice(2));

var subRecord = !!argv.s;

var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;
var mixinMongoSubrecordMethods = require("../mongo/subrecord").mixinMongoSubrecordMethods;

function Task(stageAttempt, id) {
  this.appId = stageAttempt.appId;
  this.stageId = stageAttempt.stageId;
  this.stageAttemptId = stageAttempt.id;
  this.id = id;

  if (subRecord) {
    this.super = stageAttempt;
    this.superKey = ['tasks', id, ''].join('.');
    this.set('id', id);
  } else {
    this.init([ 'appId', 'stageId', 'stageAttemptId', 'id' ]);
  }
}

if (subRecord) {
  mixinMongoSubrecordMethods(Task, "Task");
} else {
  mixinMongoMethods(Task, "Task", "Tasks");
}

module.exports.Task = Task;
