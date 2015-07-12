
var async = require('async');
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
  this.init(['id']);

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

var appsInFlight = {};

App.prototype.hydrate = function(cb) {
  function fetch(collName) {
    return function (cb) {
      colls[collName].find({appId: id}).toArray(cb);
    };
  }

  var self = this;
  var id = self.id;

  async.parallel(
        {
          jobs: fetch('Jobs'),
          stages: fetch('Stages'),
          stageAttempts: fetch('StageAttempts'),
          executors: fetch('Executors'),
          rdds: fetch('RDDs'),
          tasks: fetch('Tasks'),
          taskAttempts: fetch('TaskAttempts'),
          rddBlocks: fetch('RddBlocks'),
          nonRddBlocks: fetch('NonRddBlocks')
        },
        function(err, r) {
          if (err) {
            l.error("Failed to fetch records for app %s:", id, JSON.stringify(err));
          } else {
            r.jobs.forEach(function(job) {
              self.jobs[job.id] = new Job(id, job.id).fromMongo(job);
            });
            r.stages.forEach(function(stage) {
              self.stages[stage.id] = new Stage(id, stage.id).fromMongo(stage);
            });
            r.rdds.forEach(function(rdd) {
              self.rdds[rdd.id] = new RDD(id, rdd.id).fromMongo(rdd);
            });
            r.executors.forEach(function(executor) {
              self.executors[executor.id] = new Executor(id, executor.id).fromMongo(executor);
            });

            l.info(
                  "App %s: found %d jobs, %d stages, %d stage attempts, %d executors, %d rdds, %d tasks, %d task attempts, %d rdd blocks, and %d non-rdd blocks",
                  id,
                  r.jobs.length,
                  r.stages.length,
                  r.stageAttempts.length,
                  r.executors.length,
                  r.rdds.length,
                  r.tasks.length,
                  r.taskAttempts.length,
                  r.rddBlocks.length,
                  r.nonRddBlocks.length
            );

            r.stageAttempts.forEach(function (stageAttempt) {
              if (!(stageAttempt.stageId in self.stages)) {
                l.error(
                      "StageAttempt %s's stageId %d not found in app %s's stages",
                      stageAttempt.id,
                      stageAttempt.stageId,
                      id,
                      r.stages.map(function (e) {
                        return e.id;
                      }).join(',')
                );
              } else {
                self.stages[stageAttempt.stageId].getAttempt(stageAttempt.id).fromMongo(stageAttempt);
              }
            });

            r.tasks.forEach(function (task) {
              if (!(task.stageId in self.stages) ||
                    !(task.stageAttemptId in self.stages[task.stageId].attempts)) {
                l.error(
                      "Task %d's stageAttept %d.%d not found in app %s's stages: %s",
                      task.id,
                      task.stageId,
                      task.stageAttemptId,
                      id,
                      r.stages.map(function (e) { return e.id; }).join(',')
                );
              } else {
                self
                      .stages[task.stageId]
                      .attempts[task.stageAttemptId]
                      .getTask(task.id)
                      .fromMongo(task);
              }
            });

            r.taskAttempts.forEach(function (taskAttempt) {
              if (!(taskAttempt.stageId in self.stages) ||
                    !(taskAttempt.stageAttemptId in self.stages[taskAttempt.stageId].attempts)) {
                l.error(
                      "TaskAttempt %d's stageAttempt %d.%d not found in app %s's stages: %s",
                      taskAttempt.id,
                      taskAttempt.stageId,
                      taskAttempt.stageAttemptId,
                      id,
                      r.stages.map(function (e) { return e.id; }).join(',')
                );
              } else {
                self
                      .stages[taskAttempt.stageId]
                      .attempts[taskAttempt.stageAttemptId]
                      .getTaskAttempt(taskAttempt.id)
                      .fromMongo(taskAttempt);
              }
            });

            r.rddBlocks.forEach(function(block) {
              if (!(block.execId in self.executors)) {
                l.error(
                      "Block %s's execId %s not found in app %s's executors: %s",
                      block.id,
                      block.execId,
                      id,
                      r.executors.map(function(e) { return e.id; }).join(',')
                );
              } else {
                self.executors[block.execId].getBlock(block.id).fromMongo(block);
              }
            });

            r.nonRddBlocks.forEach(function(block) {
              if (!(block.rddId in self.rdds)) {
                l.error(
                      "Block %s's rddId %d not found in app %s's RDDs: %s",
                      block.id,
                      block.rddId,
                      id,
                      r.rdds.map(function(e) { return e.id; }).join(',')
                );
              } else {
                self.rdds[block.rddId].getBlock(block.id).fromMongo(block);
              }
            });
          }

          cb();
        }
  );
};

function getApp(id, cb) {
  if (typeof id == 'object') {
    id = id['appId'];
  }
  if (!(id in apps)) {
    if (id in appsInFlight) {
      appsInFlight[id].push(cb);
    } else {
      // A new Spark application! "Stop the world" and page in all records that
      // have anything to do with it; when this is done we will go forth
      // recklessly in write-only mode to Mongo.
      appsInFlight[id] = [cb];
      l.info("Fetching app: ", id);
      var self = this;
      colls.Applications.findOne({ id: id }, function(err, appRecord) {
        if (err) {
          l.error("Error fetching app %s: %s", id, JSON.stringify(err));
        } else {
          l.info("Got app record: %s", id, JSON.stringify(appRecord));
          var app = new App(id).fromMongo(appRecord);
          app.hydrate(function() {
            while (appsInFlight[id].length) {
              appsInFlight[id].shift()(app);
            }
            apps[id] = app;
            delete appsInFlight[id];
          });
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
  if (!(stageId in this.stages)) {
    l.error("Stage %d not found.", stageId);
    return;
  }
  var stage = this.stages[stageId];
  if (!stage.has('jobId')) {
    l.error("Stage %d has no jobId set.", stageId);
    return;
  }
  var jobId = stage.get('jobId');
  if (!(jobId in this.jobs)) {
    l.error("Job %d not found for stage %d", jobId, stageId);
    return;
  }
  return this.jobs[jobId];
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
