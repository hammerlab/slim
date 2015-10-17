
var fs = require('fs');
var http = require('http');
var mkdirp = require('mkdirp');
var net = require('net');
var oboe = require('oboe');
var path = require('path');

var extend = require('node.extend');

var argv = require('minimist')(process.argv.slice(2));

var port = argv.P || argv.port || 8123;

var appEvictionDelay = argv.e || argv['eviction-delay'] || 10;

var colls = require('./mongo/collections');
var getApp = require('./models/app').getApp;
var evictApp = require('./models/app').evictApp;

var utils = require("./utils/utils");
var statusStr = utils.status;
var processTime = utils.processTime;

var l = require('./utils/log').l;

var PENDING = utils.PENDING;
var RUNNING = utils.RUNNING;
var FAILED = utils.FAILED;
var SUCCEEDED = utils.SUCCEEDED;
var SKIPPED = utils.SKIPPED;
var REMOVED = utils.REMOVED;
var LEAKED = utils.LEAKED;
var taskEndObj = utils.taskEndObj;

var objUtils = require("./utils/objs");
var subObjs = objUtils.subObjs;
var addObjs = objUtils.addObjs;
var maxObjs = objUtils.maxObjs;

var toSeq = objUtils.toSeq;
var removeKeySpaces = objUtils.removeKeySpaces;

var recordUtils = require("./mongo/record");
var fireUpserts = recordUtils.fireUpserts;
var upsertCb = recordUtils.upsertCb;
var upsertOpts = recordUtils.upsertOpts;

function maybeAddTotalShuffleReadBytes(metrics) {
  if (!metrics || !('ShuffleReadMetrics' in metrics)) return metrics;
  var srm = metrics['ShuffleReadMetrics'];
  srm['TotalBytesRead'] = srm['LocalBytesRead'] + srm['RemoteBytesRead'];
  return metrics;
}

function handleTaskMetrics(taskMetrics, taskAttempt) {
  var newTaskAttemptMetrics = taskMetrics;
  if (!newTaskAttemptMetrics) {
    return;
  }

  taskAttempt.setDuration();
  var grt = taskAttempt.get('GettingResultTime') || 0;
  if (grt) {
    var end = taskAttempt.get('time.end') || (moment().unix()*1000);
    newTaskAttemptMetrics.GettingResultTime = end - grt;
  }
  var duration = taskAttempt.get('duration') || 0;
  var runTime = newTaskAttemptMetrics.ExecutorRunTime || 0;
  var resultSerTime = newTaskAttemptMetrics.ResultSerializationTime || 0;
  var deserTime = newTaskAttemptMetrics.ExecutorDeserializeTime || 0;
  var schedulerDelayTime = duration - runTime - resultSerTime - deserTime - grt;
  newTaskAttemptMetrics.SchedulerDelayTime = schedulerDelayTime;

  taskAttempt.set('metrics', newTaskAttemptMetrics, true);
}

function handleBlockUpdate(app, executor, blockInfo) {
  var blockId = blockInfo['BlockID'];

  var rddIdMatch = blockId.match(/^rdd_([0-9]+)_([0-9]+)$/);
  var rdd = null;
  var rddExecutor = null;
  var block = null;
  var blockWasCached = false;
  if (rddIdMatch) {
    var rddId = parseInt(rddIdMatch[1]);
    var blockIndex = parseInt(rddIdMatch[2]);

    rdd = app.getRDD(rddId);

    rddExecutor = rdd.getExecutor(executor).set({ host: executor.get('host'), port: executor.get('port') });

    block = rddExecutor.getBlock(blockIndex);

    if (block.isCached()) {
      blockWasCached = true;
    }
  } else {
    block = executor.getBlock(blockId);
  }

  var status = blockInfo['Status'];
  var blockIsCached = false;
  ['MemorySize', 'DiskSize', 'ExternalBlockStoreSize'].forEach(function (key) {
    if (status[key] && rdd) {
      blockIsCached = true;
    }
    block.set(key, status[key], true);
  });

  if (rdd) {
    if (blockIsCached) {
      if (!blockWasCached) {
        rddExecutor.inc('numCachedPartitions');
      }
    } else {
      if (blockWasCached) {
        rddExecutor.dec('numCachedPartitions');
      }
    }
    block.set('StorageLevel', status['StorageLevel'], true);
  }
  block.set({ host: executor.get('host'), port: executor.get('port') }, true);
}

function handleBlockUpdates(taskMetrics, app, executor) {
  var updatedBlocks = taskMetrics && taskMetrics['UpdatedBlocks'];
  if (updatedBlocks) {
    updatedBlocks.forEach(function (blockInfo) {
      handleBlockUpdate(app, executor, blockInfo);
    });
  }
}

// NOTE(ryan): this is called with Stage and StageAttempt records.
function maybeSetSkipped(app, job, stage, stageCountsKey, taskCountsKey) {
  var status = stage.get('status');
  if (status == RUNNING) {
    l.error(
          "Found unexpected status %s for %s when marking job %d complete.",
          statusStr[status],
          stage.toString(),
          job.id
    );
  } else if (!status) {
    // Will log an error if a status exists for this stage
    stage.set('status', SKIPPED);
    var skippedTasks = stage.get('taskCounts.num') || 0;
    if (job) job.inc(stageCountsKey + '.skipped').inc(taskCountsKey + '.skipped', skippedTasks);
    app.inc(stageCountsKey + '.skipped').inc(taskCountsKey + '.skipped', skippedTasks);
  }
}

var handlers = {

  SparkListenerApplicationStart: function(app, e) {
    app.fromEvent(e).set('status', RUNNING);
  },

  SparkListenerApplicationEnd: function(app, e) {
    app
          .set('time.end', processTime(e['Timestamp']))
          // NOTE(ryan): we don't get actual success/failure info via the JSON API today.
          .set('status', SUCCEEDED, true);

    setTimeout(function() {
      evictApp(app.id);
    }, appEvictionDelay * 1000);
  },

  SparkListenerJobStart: function(app, e) {
    var job = app.getJob(e);
    var numTasks = 0;

    var stageInfos = e['Stage Infos'];

    var stageNames = [];
    stageInfos.forEach(function(si) {

      var stage = app.getStage(si['Stage ID']).fromStageInfo(si).setJob(job);
      var attempt = stage.getAttempt(si['Stage Attempt ID']).fromStageInfo(si);

      si['RDD Info'].forEach(function(ri) {
        app.getRDD(ri).fromRDDInfo(ri);
      }.bind(this));

      numTasks += si['Number of Tasks'];
      stageNames.push(stage.get('name'));
    });

    job.set({
      'time.start': processTime(e['Submission Time']),
      stageIDs: e['Stage IDs'],
      stageNames: stageNames,
      status: RUNNING,
      'taskCounts.num': numTasks,
      'taskIdxCounts.num': numTasks,
      'stageCounts.num': e['Stage IDs'].length,
      'stageIdxCounts.num': e['Stage IDs'].length,
      properties: toSeq(e['Properties'])
    });
  },

  SparkListenerJobEnd: function(app, e) {
    var job = app.getJob(e);

    var succeeded = e['Job Result']['Result'] == 'JobSucceeded';
    job
          .set({
            'time.end': processTime(e['Completion Time']),
            result: e['Job Result'],
            succeeded: succeeded,
            ended: true
          })
          .set('status', succeeded ? SUCCEEDED : FAILED, true);

    // Mark job's stages, and any relevant "pending" attempts, as "skipped".
    job.get('stageIDs').map(function(sid) {
      var stage = app.getStage(sid);
      maybeSetSkipped(app, job, stage, 'stageIdxCounts', 'taskIdxCounts');
      for (var attemptId in stage.attempts) {
        var attempt = stage.attempts[attemptId];
        maybeSetSkipped(app, job, attempt, 'stageCounts', 'taskCounts');
      }
    });
  },

  SparkListenerStageSubmitted: function(app, e) {
    var si = e['Stage Info'];

    var stage = app.getStage(si);
    var attempt = stage.getAttempt(si);
    var job = stage.job;

    var prevStageStatus = stage.get('status');
    var prevAttemptStatus = attempt.get('status');

    if (prevAttemptStatus) {
      l.error(
            "Stage attempt %d.%d being marked as RUNNING despite extant status %s",
            attempt.stageId,
            attempt.id,
            statusStr[prevAttemptStatus]
      );
    }

    stage.fromStageInfo(si).set({ properties: toSeq(e['Properties']) }).inc('attempts.num').inc('attempts.running');
    attempt.fromStageInfo(si).set({ started: true, status: RUNNING });
    if (job) job.inc('stageCounts.running');
    app.inc('stageCounts.running');

    if (prevStageStatus == SKIPPED) {
      l.warn("Stage %d marked as %s but attempt %d submitted", stage.id, statusStr[prevStageStatus], attempt.id);
    } else if (prevStageStatus == SUCCEEDED) {
      l.info("Previously succeeded stage %d starting new attempt: %d", stage.id, attempt.id);
    } else if (prevStageStatus == FAILED) {
      stage.set('status', RUNNING, true);
      if(job) job.dec('stageIdxCounts.failed').inc('stageIdxCounts.running');
      app.dec('stageIdxCounts.failed').inc('stageIdxCounts.running')
    } else if (prevStageStatus == PENDING) {
      stage.set('status', RUNNING, true);
      if(job) job.inc('stageIdxCounts.running');
      app.inc('stageIdxCounts.running');
    }
  },

  SparkListenerStageCompleted: function(app, e) {
    var si = e['Stage Info'];

    var stage = app.getStage(si);
    stage.fromStageInfo(si);
    var prevStageStatus = stage.get('status');

    var attempt = stage.getAttempt(si);

    var prevAttemptStatus = attempt.get('status');
    var newAttemptStatus = si['Failure Reason'] ? FAILED : SUCCEEDED;

    attempt.fromStageInfo(si).set({ ended: true }).set('status', newAttemptStatus, true);
    var endTime = attempt.get('time.end');

    // We often never receive TaskEnd events for TaskAttempts that are still running when the StageAttempt completes;
    // see SPARK-9038.
    // Mark such TaskAttempts as "leaked" and set their end-times now so that finished StageAttempts aren't shown as
    // having TaskAttempts still running.
    // If we subsequently receive TaskEnds for "leaked" TaskAttempts, we just let them count as succeeded or failed and
    // remove them from "leaked" purgatory.
    for (var tid in attempt.task_attempts) {
      var taskAttempt = attempt.task_attempts[tid];
      if (taskAttempt.get('status') === RUNNING) {
        if (!taskAttempt.has('time.end')) {
          taskAttempt.set('time.end', endTime, true).set('status', LEAKED, true);
        }
      }
    }

    // Port this stageAttempt's counts of "running" taskAttempts to counts of "leaked" attempts instead.
    var tasksRunning = attempt.get('taskCounts.running');
    attempt
          .dec('taskCounts.running', tasksRunning)
          .inc('taskCounts.leaked', tasksRunning);

    if (attempt.get('taskIdxCounts.running') != 0) {
      l.error(
            "%s marked completed with %d tasks still marked as having running (and no succeeded) attempts.",
            attempt.toString(),
            attempt.get('taskIdxCounts.running')
      );
    }

    var job = stage.job;

    if (prevAttemptStatus == RUNNING) {
      stage.dec('attempts.running');
      if (job) job.dec('stageCounts.running');
      app.dec('stageCounts.running');
    } else {
      l.error(
            "Got status %s for stage attempt %d.%d with existing status %s",
            statusStr[newAttemptStatus],
            stage.id,
            attempt.id,
            statusStr[prevAttemptStatus]
      );
    }

    if (newAttemptStatus == SUCCEEDED) {

      stage.inc('attempts.succeeded');
      if (job) job.inc('stageCounts.succeeded');
      app.inc('stageCounts.succeeded');

      if (prevStageStatus == SUCCEEDED) {
        l.info(
              "Ignoring stage attempt %d.%d success; stage already marked SUCCEEDED",
              attempt.stageId,
              attempt.id
        );
      } else {
        if (prevStageStatus == RUNNING) {
          if (job) job.dec('stageIdxCounts.running');
          app.dec('stageIdxCounts.running');
        } else {
          l.error(
                "Stage attempt %d.%d FAILED when stage was previously %s, not RUNNING",
                stage.id,
                attempt.id,
                statusStr[prevStageStatus]
          );
          if (prevStageStatus == FAILED) {
            if (job) job.dec('stageIdxCounts.failed');
            app.dec('stageIdxCounts.failed');
          }
        }
        stage.set('status', SUCCEEDED, true);
        if (job) job.inc('stageIdxCounts.succeeded');
        app.inc('stageIdxCounts.succeeded');
      }
    } else {
      // attempt FAILED
      stage.inc('attempts.failed');
      if (job) job.inc('stageCounts.failed');
      app.inc('stageCounts.failed');

      if (prevStageStatus == SUCCEEDED) {
        l.info(
              "Ignoring stage attempt %d.%d failure; stage already marked SUCCEEDED",
              attempt.stageId,
              attempt.id
        );
      } else {
        if (prevStageStatus == RUNNING) {
          if (job) job.dec('stageIdxCounts.running');
          app.dec('stageIdxCounts.running');
        } else {
          l.error(
                "Stage attempt %d.%d FAILED when stage was previously %s, not RUNNING",
                stage.id,
                attempt.id,
                statusStr[prevStageStatus]
          )
        }
        stage.set('status', FAILED, true);
        if (job) job.inc('stageIdxCounts.failed');
        app.inc('stageIdxCounts.failed');

        ['num', 'running', 'succeeded', 'failed'].forEach(function(key) {
          var jobKey = ['taskIdxCounts', key].join('.');
          var resetValue =
                (job && job.stageIDs || []).reduce(function(sum, stageId) {
                  return sum + (app.getStage(stageId).get('taskIdxCounts')[key] || 0);
                }, 0);
          l.info(
                "Resetting job %d 'taskIdxCounts.%s' on stage attempt %d.%d failure: %d -> %d",
                job && job.id, key, stage.id, attempt.id, job && job.get(jobKey), resetValue
          );
          if (job) job.set(jobKey, resetValue, true);
        });
      }
    }
  },

  SparkListenerTaskStart: function(app, e) {
    var stage = app.getStage(e);
    var stageAttempt = stage.getAttempt(e);

    var ti = e['Task Info'];
    var taskId = ti['Task ID'];

    var executor = app.getExecutor(ti);
    var stageExecutor =
          stageAttempt
                .getExecutor(executor)
                .set({
                  host: executor.get('host'),
                  port: executor.get('port')
                });

    var taskIndex = ti['Index'];
    var task = stageAttempt.getTask(taskIndex);
    var prevTaskStatus = task.get('status');

    var taskAttempt = stageAttempt.getTaskAttempt(taskId);
    var prevTaskAttemptStatus = taskAttempt.get('status');

    taskAttempt.fromTaskInfo(ti);

    if (prevTaskAttemptStatus) {
      var taskAttemptId = taskAttempt.get('attempt');
      l.error(
            "Unexpected TaskStart for TID %d (%d.%d:%d.%d), status: %s (%d) -> %s (%d)",
            taskId,
            stageAttempt.stageId, stageAttempt.id,
            taskIndex, taskAttemptId,
            statusStr[prevTaskAttemptStatus], prevTaskAttemptStatus,
            statusStr[RUNNING], RUNNING
      );
    } else {
      taskAttempt.set('status', RUNNING);
      stageExecutor.inc('taskCounts.running').inc('taskCounts.num');
    }

    if (prevTaskStatus == SUCCEEDED) {
      l.error(
            "Unexpected TaskStart for task index %d (%d.%d:%d.%d); already marked as SUCCEEDED",
            taskIndex,
            stageAttempt.stageId, stageAttempt.id,
            taskIndex, taskAttemptId
      )
    } else {
      task.set('status', RUNNING, true);
      if (prevTaskStatus != RUNNING) {
        stageAttempt.inc('taskIdxCounts.running');
      }
      if (prevTaskStatus == FAILED) {
        stageAttempt.dec('taskIdxCounts.failed');
      }
    }
  },

  SparkListenerTaskGettingResult: function(app, e) {
    var stage = app.getStage(e);
    var stageAttempt = stage.getAttempt(e);

    var ti = e['Task Info'];
    var taskId = ti['Task ID'];

    stageAttempt.getTaskAttempt(taskId).fromTaskInfo(ti);
  },

  SparkListenerTaskEnd: function(app, e) {
    var stage = app.getStage(e);
    var stageAttempt = stage.getAttempt(e);

    var ti = e['Task Info'];
    var taskId = ti['Task ID'];
    var taskIndex = ti['Index'];
    var taskAttemptId = ti['Attempt'];

    var taskAttempt = stageAttempt.getTaskAttempt(taskId).set({ end: taskEndObj(e['Task End Reason']) });
    var prevTaskAttemptStatus = taskAttempt.get('status');

    taskAttempt.fromTaskInfo(ti);

    if (taskAttempt.stageAttemptId != stageAttempt.id) {
      l.warn(
            "Task %d found for attempt %d, not %d; using the former",
            taskAttempt.id,
            taskAttempt.stageAttemptId,
            stageAttempt.id
      );
      stageAttempt = stage.getAttempt(taskAttempt.stageAttemptId);
    }

    var executor = app.getExecutor(ti);
    var stageExecutor =
          stageAttempt
                .getExecutor(executor)
                .set({
                  host: executor.get('host'),
                  port: executor.get('port')
                });

    var task = stageAttempt.getTask(taskIndex).set({ type: e['Task Type'] });
    var prevTaskStatus = task.get('status');

    var taskMetrics = maybeAddTotalShuffleReadBytes(removeKeySpaces(e['Task Metrics']));
    handleTaskMetrics(taskMetrics, taskAttempt);
    handleBlockUpdates(taskMetrics, app, executor);

    var succeeded = !ti['Failed'];
    var status = succeeded ? SUCCEEDED : FAILED;
    var taskCountKey = succeeded ? 'taskCounts.succeeded' : 'taskCounts.failed';
    var taskIdxCountKey = succeeded ? 'taskIdxCounts.succeeded' : 'taskIdxCounts.failed';

    // Update stageAttempt's histograms of task indexes' numbers of failing/failed attempts.
    var prevNumFailed = task.get('failed') || 0;

    // If this task has previously failed, then decrement stageAttempt's 'failed' and 'failing' counts before
    // incrementing them to reflect the new number of times that the task's attempts have failed.
    //
    // Note that "failing" should only be decremented if the task's status was anything but SUCCEEDED; even if its
    // previous status was, say, RUNNING, `prevNumFailed > 0` means that the task was being *re-run* after at least one
    // previous failure, and was in a "failing" state. We don't currently distinguish tasks that are RUNNING from those
    // that are being ["re-run" after a failure].
    if (prevNumFailed) {
      stageAttempt.dec('failed.' + prevNumFailed);
      if (prevTaskStatus != SUCCEEDED) {
        stageAttempt.dec('failing.' + prevNumFailed);
      }
    }
    if (!succeeded) {
      task.inc('failed');
      var numFailed = prevNumFailed + 1;
      stageAttempt
            .inc('failed.' + numFailed)
            .inc('failing.' + numFailed);
    }

    // Update TaskAttempt's status and various downstream records' 'taskCounts' objects.
    if (prevTaskAttemptStatus == RUNNING) {
      taskAttempt.set('status', status, true);
      stageExecutor.dec('taskCounts.running').inc(taskCountKey);
    } else if (prevTaskAttemptStatus == LEAKED) {
      taskAttempt.set('status', status, true);
      stageExecutor.dec('taskCounts.leaked').inc(taskCountKey);
    } else {
      l.error(
            "%s: Unexpected TaskEnd for %d (%s:%s), status: %s (%d) -> %s (%d)",
            taskAttempt.toString(),
            taskId,
            stageAttempt.stageId + "." + stageAttempt.id,
            taskIndex + "." + taskAttemptId,
            statusStr[prevTaskAttemptStatus], prevTaskAttemptStatus,
            statusStr[status], status
      );

      // NOTE(ryan): we "should" never get here, but make an attempt to do sane things in case we do.
      // We can (only?) get here if the Spark LiveListenerBus drops events, which it does if it gets
      // 10,000 events behind; if it drops a TaskStart event then we could receive a TaskEnd for an
      // attempt that we know nothing about and that has an empty status.
      if (succeeded && prevTaskAttemptStatus != SUCCEEDED) {
        taskAttempt.set('status', status, true);
        stageExecutor.inc(taskCountKey);
        if (prevTaskAttemptStatus == FAILED) {
          // I know what you're thinking: "how could a single task *attempt* have previously failed but now succeeded?
          // This would require two TaskEnd events for the same TaskAttempt!" And to that I say check out SPARK-10551.
          stageExecutor.dec('taskCounts.failed');
        }
      }
    }

    // Update Task's status and various downstream records' 'taskIdxCounts' objects.
    if (prevTaskStatus == RUNNING) {
      task.set('status', status, true);
      stageAttempt.dec('taskIdxCounts.running').inc(taskIdxCountKey);
    } else if (prevTaskStatus == LEAKED) {
      task.set('status', status, true);
      stageAttempt.dec('taskIdxCounts.leaked').inc(taskIdxCountKey);
    } else if (prevTaskStatus == SUCCEEDED) {
      var logFn = succeeded ? l.info : l.warn;
      logFn(
            "Ignoring status %s for task %d (%s:%s) because existing status is SUCCEEDED",
            statusStr[status],
            taskId,
            stageAttempt.stageId + "." + stageAttempt.id,
            taskIndex + "." + taskAttemptId
      )
    } else {
      // This task should have been marked as "RUNNING" before we received a TaskEnd about an attempt of it; if we're
      // here it likely means that a TaskStart event for this attempt was dropped. See the above comment about
      // LiveListenerBus.
      l.error(
            "Got TaskEnd for %d (%s:%s) with previous task status %s",
            taskId,
            stageAttempt.stageId + "." + stageAttempt.id,
            taskIndex + "." + taskAttemptId,
            statusStr[prevTaskStatus]
      );

      // Try to do some sane things here anyway: if this attempt succeeded, then mark the task succeeded if it's not
      // already marked as such (e.g. by another attempt having already returned successfully).
      if (succeeded && prevTaskStatus != SUCCEEDED) {
        task.set('status', status, true);
        stageAttempt.inc(taskIdxCountKey);
        if (prevTaskStatus == FAILED) {
          // In particular, if
          stageAttempt.dec('taskIdxCounts.failed');
        }
      }
    }
  },

  SparkListenerEnvironmentUpdate: function(app, e) {
    colls.collections.Environment.findOneAndUpdate(
          { appId: e['appId'] },
          {
            $set: {
              jvm: toSeq(e['JVM Information']),
              spark: toSeq(e['Spark Properties']),
              system: toSeq(e['System Properties']),
              classpath: toSeq(e['Classpath Entries'])
            }
          },
          upsertOpts,
          upsertCb("Environment")
    );
    app.set('maxTaskFailures', e['JVM Information']['spark.task.maxFailures'] || 4);
  },
  SparkListenerBlockManagerAdded: function(app, e) {
    app
          .getExecutor(e)
          .set({
            maxMem: e['Maximum Memory'],
            host: e['Block Manager ID']['Host'],
            port: e['Block Manager ID']['Port']
          }, true)
          .set({
            'time.start': processTime(e['Timestamp']),
            'status': RUNNING
          }, true);
    app
          .inc('blockManagerCounts.num')
          .inc('blockManagerCounts.running');
  },
  SparkListenerBlockManagerRemoved: function(app, e) {
    var executor = app.getExecutor(e);

    executor.set({
      host: e['Block Manager ID']['Host'],
      port: e['Block Manager ID']['Port'],
      'time.end': processTime(e['Timestamp']),
      'status': REMOVED
    }, true);

    app
          .dec('blockManagerCounts.running')
          .inc('blockManagerCounts.removed');

    for (var rddId in app.rdds) {
      app.rdds[rddId].removeExecutor(executor);
    }
  },

  SparkListenerUnpersistRDD: function(app, e) {
    app.getRDD(e['RDD ID']).unpersist();
  },

  SparkListenerExecutorAdded: function(app, e) {
    var ei = e['Executor Info'];
    app
          .getExecutor(e)
          .set({
            host: ei['Host'],
            cores: ei['Total Cores'],
            urls: ei['Log Urls']
          })
          .set({
            'time.start': processTime(e['Timestamp']),
            'status': RUNNING
          }, true);
    app
          .inc('executorCounts.num')
          .inc('executorCounts.running');
  },

  SparkListenerExecutorRemoved: function(app, e) {
    app
          .getExecutor(e)
          .set('reason', e['Removed Reason'])
          .set({
            'status': REMOVED,
            'time.end': processTime(e['Timestamp'])
          }, true);
    app
          .dec('executorCounts.running')
          .inc('executorCounts.removed');
  },
  SparkListenerLogStart: function(app, e) {
    // Spark EventListenerBus doesn't actually send this event.
  },
  SparkListenerExecutorMetricsUpdate: function(app, e) {
    if (!e['Metrics Updated']) {
      // Depend on JsonRelay to filter out empty MetricsUpdate events.
      l.error("Got SparkListenerExecutorMetricsUpdate event with empty 'Metrics Updated': ", JSON.stringify(e));
      return;
    }
    e['Metrics Updated'].map(function(m) {
      var stage = app.getStage(m);
      var stageAttempt = stage.getAttempt(m);
      var taskAttempt = stageAttempt.getTaskAttempt(m);
      var taskMetrics = maybeAddTotalShuffleReadBytes(removeKeySpaces(m['Task Metrics']));
      handleTaskMetrics(taskMetrics, taskAttempt);
    });
  },

  SparkListenerBlockUpdated: function(app, e) {
    var executor = app.getExecutor(e);
    handleBlockUpdate(app, executor, e);
  }
};

function handleEvent(e) {
  l.debug('Got data: ', e);
  if ('Event' in e) {
    getApp(e, function(app) {
      handlers[e['Event']](app, e);
      fireUpserts();
    });
  }
}

function Server(mongoUrl) {
  l.info("Starting slim v1.3.0");
  if (argv.log) {
    var lastSlashIdx = argv.log.lastIndexOf('/');
    if (lastSlashIdx >= 0) {
      var dir = path.dirname(argv.log);
      l.info("Creating event-log directory:", dir);
      mkdirp.sync(dir);
    }
  }
  var logFd = argv.log && fs.openSync(argv.log, 'wx');

  colls.init(mongoUrl, function(err) {
    if (err) {
      throw new Error("Failed to initialize Mongo:", JSON.stringify(err));
    }
    var server = net.createServer(function (c) {
      l.warn("client connected");
      var setupOboe = function () {
        l.debug("Registering oboe");
        oboe(c).node('!', function (e) {
          if (logFd) {
            fs.writeSync(logFd, JSON.stringify(e) + '\n');
          }
          handleEvent(e);
        }).fail(function (e) {
          l.error("Oboe caught error:", e.thrown.stack);
        });
      };
      setupOboe();

      c.on('end', function () {
        l.warn("client disconnected");
      })
    });
    server.listen(port, function () {
      l.warn("Server listening on: http://localhost:%s", port);
    });
  });
}

module.exports.Server = Server;
module.exports.handleEvent = handleEvent;
