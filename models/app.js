
var async = require('async');
var colls = require('../mongo/collections').collections;

var l = require('../utils/log').l;

var utils = require("../utils/utils");
var RUNNING = utils.RUNNING;

var processTime = utils.processTime;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

var Job = require('./job').Job;
var Stage = require('./stage').Stage;
var RDD = require('./rdd').RDD;
var Executor = require('./executor').Executor;
var getExecutorId = require('./executor').getExecutorId;

var apps = {};

function App(id) {
  this.id = id;
  this.init(['id']);

  this.jobs = {};
  this.stages = {};
  this.rdds = {};
  this.executors = {};

  // Map of all task attempts in this app, redundant with stage-attempts' maps,
  // to work around a bug in Spark where a TaskEnd will imply that a task
  // belongs to a different (newer) attempt of its stage than it does / was
  // `TaskStart`ed with.
  this.task_attempts = {};
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
          executorThreadDumps: fetch('ExecutorThreadDumps'),
          stageExecutors: fetch('StageExecutors'),
          rdds: fetch('RDDs'),
          rddExecutors: fetch('RDDExecutors'),
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
              if (job.id === null) {
                log.error("job.id === null when fetching records for app %s: %s", id, job.toString());
              }
              self.jobs[job.id] = new Job(self, job.id).fromMongo(job);
            });
            r.stages.forEach(function(stage) {
              self.stages[stage.id] = new Stage(self, stage.id).fromMongo(stage);
            });
            for (var jobId in self.jobs) {
              var job = self.jobs[jobId];
              (job.get('stageIDs') || []).forEach(function(sid) {
                if (!(sid in self.stages)) {
                  l.error("Missing stage %d in job %d", sid, job.id);
                } else {
                  var stage = self.stages[sid];
                  stage.job = job;
                  stage.set('jobId', job.id);
                }
              });
            }

            r.rdds.forEach(function(rdd) {
              self.rdds[rdd.id] = new RDD(id, rdd.id).fromMongo(rdd);
            });
            r.executors.forEach(function(executor) {
              self.executors[executor.id] = new Executor(id, executor.id).fromMongo(executor);
            });
            r.executorThreadDumps.forEach(function(threadDump) {
              if (!(threadDump.execId in self.executors)) {
                l.error(
                  "Executor thread %s refers to non-existing executor %s in app %s",
                  threadDump.threadId,
                  threadDump.execId,
                  id
                );
              }

              self.executors[threadDump.execId].getThreadDump(threadId).fromMongo(threadDump);
            })

            l.info(
                  [
                    "Found app %s:",
                    "%d jobs",
                    "%d stages",
                    "%d stage attempts",
                    "%d executors",
                    "%d executor thread dumps",
                    "%d stage-executors",
                    "%d rdds",
                    "%d rdd-executors",
                    "%d tasks",
                    "%d task attempts",
                    "%d rdd blocks",
                    "%d non-rdd blocks"
                  ].join('\n\t'),
                  id,
                  r.jobs.length,
                  r.stages.length,
                  r.stageAttempts.length,
                  r.executors.length,
                  r.executorThreadDumps.length,
                  r.stageExecutors.length,
                  r.rdds.length,
                  r.rddExecutors.length,
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
                      r.stages.map(function (e) {
                        return e.id;
                      }).join(',')
                );
              } else {
                self
                      .stages[taskAttempt.stageId]
                      .attempts[taskAttempt.stageAttemptId]
                      .getTaskAttempt(taskAttempt.id)
                      .fromMongo(taskAttempt)
                      .setExecutors();
              }
            });

            for (var sid in self.stages) {
              var stage = self.stages[sid];
              for (var said in stage.attempts) {
                stage.attempts[said].initMetrics();
              }
            }

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

            r.stageExecutors.forEach(function(stageExecutor) {
              if (!(stageExecutor.stageId in self.stages) ||
                    !(stageExecutor.stageAttemptId in self.stages[stageExecutor.stageId].attempts)) {
                l.error(
                      "StageExecutor %s's stageId %d.%d not found in app %s's stages: %s",
                      stageExecutor.id,
                      stageExecutor.stageId,
                      stageExecutor.stageAttemptId,
                      id,
                      r.stages.map(function(e) { return e.id; }).join(',')
                );
              } else {
                self
                      .stages[stageExecutor.stageId]
                      .attempts[stageExecutor.stageAttemptId]
                      .getExecutor(stageExecutor.execId)
                      .fromMongo(stageExecutor);
              }
            });

            r.rddExecutors.forEach(function(rddExecutor) {
              if (!(rddExecutor.rddId in self.rdds)) {
                l.error(
                      "RDDExecutor %s's rddId %d not found in app %s's RDDs: %s",
                      rddExecutor.id,
                      rddExecutor.rddId,
                      id,
                      r.rdds.map(function(e) { return e.id; }).join(',')
                );
              } else {
                self.rdds[rddExecutor.rddId].getExecutor(rddExecutor.execId).fromMongo(rddExecutor);
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

function evictApp(id) {
  if (!(id in apps)) {
    l.error("Failed to evict missing app: %s", id);
    return;
  }
  l.info("Evicting app: %s", id);
  delete apps[id];
}

App.prototype.getJob = function(jobId) {
  if (typeof jobId == 'object') {
    jobId = jobId['Job ID'];
  }
  if (!(jobId in this.jobs)) {
    this.jobs[jobId] = new Job(this, jobId);
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
    this.stages[stageId] = new Stage(this, stageId);
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
  executorId = getExecutorId(executorId);
  if (!(executorId in this.executors)) {
    this.executors[executorId] = new Executor(this.id, executorId);
  }
  return this.executors[executorId];
};

module.exports.apps = apps;
module.exports.App = App;
module.exports.getApp = getApp;
module.exports.evictApp = evictApp;
