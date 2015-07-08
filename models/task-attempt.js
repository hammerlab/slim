
var argv = require('minimist')(process.argv.slice(2));

var subRecord = !!argv.s;

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

var execsById = {};
var Executors = null;

TaskAttempt.prototype.setHostPort = function() {
  var eid = this.get('execId');
  if (eid && (!this.get('host') || !this.get('port'))) {
    if (!(this.appId in execsById)) {
      execsById[this.appId] = {};
    }
    var appExecsById = execsById[this.appId];
    if (eid in appExecsById) {
      var e = appExecsById[eid];
      this.set({ host: e.host, port: e.port });
    } else {
      if (!Executors) {
        Executors = require("../mongo/collections").Executors;
      }
      Executors.findOne({ appId: this.appId, id: eid }, function(err, e) {
        if (err) {
          l.error("Failed to fetch executor %s in app %s: ", eid, this.appId, err);
        } else {
          appExecsById[eid] = { host: e.host, port: e.port };
          this.set({ host: e.host, port: e.port }).upsert();
        }
      }.bind(this));
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
