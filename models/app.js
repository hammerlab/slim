
var colls = require('../mongo/collections');

var l = require('../utils/log').l;

var utils = require("../utils/utils");
var processTime = utils.processTime;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

var Job = require('./job').Job;
var Stage = require('./stage').Stage;
var RDD = require('./rdd').RDD;
var Executor = require('./executor').Executor;

var apps = {};

function App(id) {
  this.id = id;
  this.findObj = { id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;
  this.key = [ "app", id ].join('-');
  this.applyRateLimit = true;

  this.jobs = {};
  this.stages = {};
  this.rdds = {};
  this.executors = {};

  this.stageIDstoJobIDs = {};
}

mixinMongoMethods(App, "Application", "Applications");

App.prototype.fromEvent = function(e) {
  return this.set({
    name: e['App Name'],
    'time.start': processTime(e['Timestamp']),
    user: e['User'],
    attempt: e['App Attempt ID']
  });
};

function appFromMongoRecord(app, cb) {
  var a = new App(app.id);
  for (var k in app) {
    if (k != 'id' && k != '_id') {
      a.propsObj[k] = app[k];
    }
  }
  a.syncExecutors(cb);
}

App.prototype.syncExecutors = function(cb) {
  colls.Executors.find({ appId: this.id }).toArray(function(err, executors) {
    if (err) {
      l.error("Error fetching executors for app %s: %s", this.id, JSON.stringify(err));
    } else {
      var keys = [];
      for (var k in executors) {
        keys.push([k, executors[k]]);
      }
      executors.map(function(e) {
        this.executors[e.id] = this.executorFromMongoRecord(e);
      }.bind(this));
      cb(this);
    }
  }.bind(this));
};

App.prototype.executorFromMongoRecord = function(executor) {
  var e = new Executor(this.id, executor.id);
  for (var k in executor) {
    if (k != 'id' && k != 'appId' && k != '_id') {
      e.propsObj[k] = executor[k];
    }
  }
  return e;
};

var appsInFlight = {};

function getApp(id, cb) {
  if (typeof id == 'object') {
    id = id['appId'];
  }
  if (!(id in apps)) {
    if (id in appsInFlight) {
      appsInFlight[id].push(cb);
    } else {
      appsInFlight[id] = [cb];
      l.info("Fetching app: ", id);
      colls.Applications.findOne({ id: id }, function(err, val) {
        if (err) {
          l.error("Error fetching app %s: %s", id, JSON.stringify(err));
        } else {
          var a = val;
          l.info("Got app %s", id, JSON.stringify(a));
          if (a) {
            appFromMongoRecord(a, function (app) {
              apps[id] = app;
              appsInFlight[id].map(function(appCb) {
                appCb(app);
              });
              delete appsInFlight[id];
            })
          } else {
            var app = new App(id);
            apps[id] = app;
            appsInFlight[id].map(function(appCb) {
              appCb(app);
            });
            delete appsInFlight[id];
          }
        }
      });
    }
  } else {
    cb(apps[id]);
  }
}

App.prototype.getJob = function(jobId) {
  if (typeof jobId == 'object') {
    jobId = jobId['Job ID'];
  }
  if (!(jobId in this.jobs)) {
    this.jobs[jobId] = new Job(this.id, jobId);
  }
  return this.jobs[jobId];
};

App.prototype.getJobByStageId = function(stageId) {
  if (!(stageId in this.stageIDstoJobIDs)) {
    throw new Error("No job found for stage " + stageId);
  }
  return this.jobs[this.stageIDstoJobIDs[stageId]];
};

App.prototype.getStage = function(stageId) {
  if (typeof stageId == 'object') {
    if ('Stage ID' in stageId) {
      stageId = stageId['Stage ID'];
    } else if ('Stage Info' in stageId) {
      stageId = stageId['Stage Info']['Stage ID'];
    } else {
      throw new Error("Invalid argument to App.getStage: " + stageId);
    }
  }
  if (!(stageId in this.stages)) {
    this.stages[stageId] = new Stage(this.id, stageId);
  }
  return this.stages[stageId];
};

App.prototype.getRDD = function(rddId) {
  if (typeof rddId == 'object') {
    rddId = rddId['RDD ID'];
  }
  if (!(rddId in this.rdds)) {
    this.rdds[rddId] = new RDD(this.id, rddId);
  }
  return this.rdds[rddId];
};

App.prototype.getExecutor = function(executorId) {
  if (typeof executorId == 'object') {
    if ('Block Manager ID' in executorId) {
      executorId = executorId['Block Manager ID'];
    }
    executorId = executorId['Executor ID'];
  }
  if (executorId.match(/^[0-9]+$/)) {
    executorId = parseInt(executorId);
  }
  if (!(executorId in this.executors)) {
    this.executors[executorId] = new Executor(this.id, executorId);
  }
  return this.executors[executorId];
};

module.exports.apps = apps;
module.exports.App = App;
module.exports.getApp = getApp;
