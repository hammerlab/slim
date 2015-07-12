
var argv = require('minimist')(process.argv.slice(2));

var l = require("../utils/log").l;
var subRecord = !!argv.s;

var apps = null;
var processTime = require("../utils/utils").processTime;
var removeKeyDots = require("../utils/objs").removeKeyDots;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;
var mixinMongoSubrecordMethods = require("../mongo/subrecord").mixinMongoSubrecordMethods;

function TaskAttempt(stageAttempt, id) {
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

TaskAttempt.prototype.fromTaskInfo = function(ti) {
  this.set({
    'time.start': processTime(ti['Launch Time']),
    'time.end': processTime(ti['Finish Time']),
    execId: ti['Executor ID'],
    host: ti['Host'],
    locality: ti['Locality'],
    speculative: ti['Speculative'],
    gettingResultTime: processTime(ti['Getting Result Time']),
    index: ti['Index'],
    attempt: ti['Attempt']
  }).set('accumulables', removeKeyDots(ti['Accumulables']), true).setDuration();
};

if (subRecord) {
  mixinMongoSubrecordMethods(TaskAttempt, "TaskAttempt");
} else {
  mixinMongoMethods(TaskAttempt, "TaskAttempt", "TaskAttempts");
}

module.exports.TaskAttempt = TaskAttempt;
