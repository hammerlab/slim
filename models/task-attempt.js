
var argv = require('minimist')(process.argv.slice(2));

var l = require("../utils/log").l;
var subRecord = !!argv.s;

var apps = null;
var processTime = require("../utils/utils").processTime;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;
var mixinMongoSubrecordMethods = require("../mongo/subrecord").mixinMongoSubrecordMethods;

function TaskAttempt(appId, stageAttempt, id) {
  this.appId = appId;
  this.stageId = stageAttempt.stageId;
  this.stageAttemptId = stageAttempt.id;
  this.id = id;

  if (subRecord) {
    this.super = stageAttempt;
    this.superKey = ['tasks', id, ''].join('.');
    this.set('id', id);
  } else {
    this.applyRateLimit = true;
    this.findObj = {appId: appId, id: id};
    this.propsObj = {};
    this.toSyncObj = {};
    this.set({
      stageId: this.stageId,
      stageAttemptId: this.stageAttemptId
    });
    this.dirty = true;
  }
}

TaskAttempt.prototype.fromTaskInfo = function(ti) {
  this.set({
    'time.start': processTime(ti['Launch Time']),
    'time.end': processTime(ti['Finish Time']),
    execId: ti['Executor ID'],
    //host: ti['Host'],  // redundant with exec ID...
    locality: ti['Locality'],
    speculative: ti['Speculative'],
    gettingResultTime: processTime(ti['Getting Result Time']),
    failed: ti['Failed'],
    accumulables: ti['Accumulables'],
    index: ti['Index'],
    attempt: ti['Attempt']
  }).setDuration().setHostPort();
};

TaskAttempt.prototype.setHostPort = function() {
  var eid = this.get('execId');
  if (eid && (!this.get('host') || !this.get('port'))) {
    if (!apps) {
      apps = require("./app").apps;
    }
    var app = apps[this.appId];
    if (!app) {
      l.error(
            "App %s not found for task attempt %d (stage %d.%d, task idx %d.%d): ",
            this.appId,
            this.id,
            this.stageId,
            this.stageAttemptId,
            this.get('index'),
            this.get('attempt')
      );
    } else {
      var e = app.getExecutor(eid);
      var host = e.get('host');
      var port = e.get('port');
      if (!host || !port) {
        l.error("Executor %s in app %s missing host/port: %s:%s. %s", eid, this.appId, host, port, JSON.stringify(e));
      } else {
        this.set({ host: host, port: port });
      }
    }
  }
  return this;
};

if (subRecord) {
  mixinMongoSubrecordMethods(TaskAttempt, "TaskAttempt");
} else {
  mixinMongoMethods(TaskAttempt, "TaskAttempt", "TaskAttempts");
}

module.exports.TaskAttempt = TaskAttempt;
