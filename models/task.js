
var subRecord = false;

var mixinMongoMethods = require("../utils").mixinMongoMethods;
var mixinMongoSubrecordMethods = require("../utils").mixinMongoSubrecordMethods;

function Task(appId, stage, id) {
  this.appId = appId;
  this.stageId = stage.id;
  this.id = id;

  if (subRecord) {
    this.super = stage;
    this.superKey = ['tasks', id, ''].join('.');
    this.set('id', id);
  } else {
    this.findObj = {appId: appId, stageId: this.stageId, id: id};
    this.propsObj = {};
    this.toSyncObj = {};
    this.dirty = true;
  }
}

if (subRecord) {
  mixinMongoSubrecordMethods(Task, "Task");
} else {
  mixinMongoMethods(Task, "Task", "Tasks");
}

module.exports.Task = Task;
