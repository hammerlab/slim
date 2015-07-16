
var http = require('http');
var net = require('net');
var oboe = require('oboe');

var extend = require('node.extend');

var argv = require('minimist')(process.argv.slice(2));

var mongoPort = argv.p || argv['mongo-port'] || 3001;
var mongoHost = argv.h || argv['mongo-host'] || 'localhost';
var mongoDb = argv.d || argv['mongo-db'] || 'meteor';
var mongoUrl = argv.m || argv['mongo-url'] || ('mongodb://' + mongoHost + ':' + mongoPort + '/' + mongoDb);
var url = 'mongodb://localhost:27017/spree';

var port = argv.P || argv.port || 8123;

var colls = require('./mongo/collections');
var getApp = require('./models/app').getApp;

var utils = require("./utils/utils");
var statusStr = utils.status;
var processTime = utils.processTime;

var l = require('./utils/log').l;

var PENDING = utils.PENDING;
var RUNNING = utils.RUNNING;
var FAILED = utils.FAILED;
var SUCCEEDED = utils.SUCCEEDED;
var SKIPPED = utils.SKIPPED;

var objUtils = require("./utils/objs");
var subObjs = objUtils.subObjs;
var addObjs = objUtils.addObjs;
var maxObjs = objUtils.maxObjs;

var toSeq = objUtils.toSeq;
var removeKeySpaces = objUtils.removeKeySpaces;

var recordUtils = require("./mongo/record");
var upsertCb = recordUtils.upsertCb;
var upsertOpts = recordUtils.upsertOpts;

function maybeAddTotalShuffleReadBytes(metrics) {
  if (!metrics || !('ShuffleReadMetrics' in metrics)) return metrics;
  var srm = metrics['ShuffleReadMetrics'];
  srm['TotalBytesRead'] = srm['LocalBytesRead'] + srm['RemoteBytesRead'];
  return metrics;
}

function handleTaskMetrics(taskMetrics, app, job, stageAttempt, executor, stageExecutor, taskAttempt) {
  var prevTaskAttemptMetrics = taskAttempt.get('metrics');
  var newTaskAttemptMetrics = taskMetrics;

  taskAttempt.set('metrics', newTaskAttemptMetrics, true);
  job.setDuration('totalJobDuration', app);

  var taskAttemptMetricsDiff = { metrics: subObjs(newTaskAttemptMetrics, prevTaskAttemptMetrics) };

  app.inc(taskAttemptMetricsDiff);
  executor.inc(taskAttemptMetricsDiff);
  stageExecutor.inc(taskAttemptMetricsDiff);
  stageAttempt.inc(taskAttemptMetricsDiff);
  job.inc(taskAttemptMetricsDiff);
}

function handleBlockUpdates(taskMetrics, app, executor) {
  var updatedBlocks = taskMetrics && taskMetrics['UpdatedBlocks'];
  var rdds = [];
  var rddExecutors = [];
  var blocks = [];
  if (updatedBlocks) {
    updatedBlocks.forEach(function (blockInfo) {
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
        rdds.push(rdd);

        rddExecutor = rdd.getExecutor(executor.id).set({ host: executor.get('host'), port: executor.get('port') });
        rddExecutors.push(rddExecutor);

        block = rdd.getBlock(blockIndex).set('execId', executor.id, true).addToSet('execIds', executor.id);
        blocks.push(block);

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
        var delta = status[key] - (block.get(key) || 0);
        executor.inc(key, delta);
        app.inc(key, delta);
        if (rdd) {
          rdd.inc(key, delta);
          rddExecutor.inc(key, delta);
        }
        block.set(key, status[key], true);
      });
      if (!blockIsCached) {
        if (blockWasCached) {
          executor.dec('numBlocks');
          if (rdd) {
            rdd
                  .dec("numCachedPartitions")
                  .set('fractionCached', (rdd.get('numCachedPartitions') || 0) / rdd.get('numPartitions'), true);
            rddExecutor.dec('numBlocks');
          }
        }
      } else {
        if (!blockWasCached) {
          executor.inc('numBlocks');
          if (rdd) {
            rdd
                  .inc("numCachedPartitions")
                  .set('fractionCached', (rdd.get('numCachedPartitions') || 0) / rdd.get('numPartitions'), true);
            rddExecutor.inc('numBlocks');
          }
        }
      }
      if (rdd) {
        block.set('StorageLevel', status['StorageLevel'], true);
      }
      block.set({ host: executor.get('host'), port: executor.get('port') }, true);
    });
  }
  rdds.forEach(function(rdd) { rdd.upsert(); });
  rddExecutors.forEach(function(rddExecutor) { rddExecutor.upsert(); });
  blocks.forEach(function(block) { block.upsert(); });
}

var handlers = {

  SparkListenerApplicationStart: function(app, e) {
    app.fromEvent(e).upsert();
  },

  SparkListenerApplicationEnd: function(app, e) {
    app.set('time.end', processTime(e['Timestamp'])).upsert();
  },

  SparkListenerJobStart: function(app, e) {
    var job = app.getJob(e);
    var numTasks = 0;

    var stageInfos = e['Stage Infos'];

    stageInfos.forEach(function(si) {

      var stage = app.getStage(si['Stage ID']).fromStageInfo(si).set('jobId', job.id).upsert();

      var attempt = stage.getAttempt(si['Stage Attempt ID']).fromStageInfo(si).setJob(job).upsert();

      si['RDD Info'].forEach(function(ri) {
        app.getRDD(ri).fromRDDInfo(ri).upsert();
      }.bind(this));

      numTasks += si['Number of Tasks'];
    });

    job.set({
      'time.start': processTime(e['Submission Time']),
      stageIDs: e['Stage IDs'],
      'taskCounts.num': numTasks,
      'taskIdxCounts.num': numTasks,
      'stageCounts.num': e['Stage IDs'].length,
      'stageIdxCounts.num': e['Stage IDs'].length,
      properties: toSeq(e['Properties'])
    }).upsert();

  },

  SparkListenerJobEnd: function(app, e) {
    var job = app.getJob(e);

    job
          .set({
            'time.end': processTime(e['Completion Time']),
            result: e['Job Result'],
            succeeded: e['Job Result']['Result'] == 'JobSucceeded',
            ended: true
          })
          .upsert();

    job.get('stageIDs').map(function(sid) {
      var stage = app.getStage(sid);
      var status = stage.get('status');
      if (status == RUNNING || status == FAILED) {
        l.err("Found unexpected status " + status + " for stage " + stage.id + " when marking job " + job.id + " complete.");
      } else if (!status) {
        // Will log an error if a status exists for this stage
        stage.set('status', SKIPPED).upsert();
      }
    });
  },

  SparkListenerStageSubmitted: function(app, e) {
    var si = e['Stage Info'];

    var stage = app.getStage(si);
    var attempt = stage.getAttempt(si);
    var job = app.getJobByStageId(stage.id);

    var prevStageStatus = stage.get('status');
    var prevAttemptStatus = attempt.get('status');

    if (prevAttemptStatus) {
      l.err(
            "Stage attempt %d.%d being marked as RUNNING despite extant status %s",
            attempt.stageId,
            attempt.id,
            statusStr[prevAttemptStatus]
      );
    }

    stage.fromStageInfo(si).set({ properties: toSeq(e['Properties']) }).inc('attempts.num').inc('attempts.running');
    attempt.fromStageInfo(si).set({ started: true, status: RUNNING });
    job.inc('stageCounts.running');

    if (prevStageStatus == SUCCEEDED || prevStageStatus == SKIPPED) {
      l.error("Stage %d marked as %s but attempt %d submitted", stage.id, statusStr[prevStageStatus], attempt.id);
    } else if (prevStageStatus == FAILED) {
      stage.set('status', RUNNING);
      job.dec('stageIdxCounts.failed').inc('stageIdxCounts.running')
    } else if (prevStageStatus == PENDING) {
      stage.set('status', RUNNING);
      job.inc('stageIdxCounts.running');
    }

    stage.upsert();
    attempt.upsert();
    job.upsert();
    app.upsert();
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

    var job = app.getJobByStageId(stage.id);

    if (prevAttemptStatus == RUNNING) {
      stage.dec('attempts.running');
      job.dec('stageCounts.running');
    } else {
      l.err(
            "Got status %s for stage attempt %d.%d with existing status %s",
            statusStr[newAttemptStatus],
            stage.id,
            attempt.id,
            statusStr[prevAttemptStatus]
      );
    }

    if (newAttemptStatus == SUCCEEDED) {

      stage.inc('attempts.succeeded');
      job.inc('stageCounts.succeeded');

      if (prevStageStatus == SUCCEEDED) {
        l.info(
              "Ignoring stage attempt %d.%d success; stage already marked SUCCEEDED",
              attempt.stageId,
              attempt.id
        );
      } else {
        if (prevStageStatus == RUNNING) {
          job.dec('stageIdxCounts.running');
        } else {
          l.error(
                "Stage attempt %d.%d FAILED when stage was previously %s, not RUNNING",
                stage.id,
                attempt.id,
                statusStr[prevStageStatus]
          );
          if (prevStageStatus == FAILED) {
            job.dec('stageIdxCounts.failed');
          }
        }
        stage.set('status', SUCCEEDED, true);
        job.inc('stageIdxCounts.succeeded');
      }
    } else {
      // attempt FAILED
      stage.inc('attempts.failed');
      job.inc('stageCounts.failed');

      if (prevStageStatus == SUCCEEDED) {
        l.info(
              "Ignoring stage attempt %d.%d failure; stage already marked SUCCEEDED",
              attempt.stageId,
              attempt.id
        );
      } else {
        if (prevStageStatus == RUNNING) {
          job.dec('stageIdxCounts.running');
        } else {
          l.error(
                "Stage attempt %d.%d FAILED when stage was previously %s, not RUNNING",
                stage.id,
                attempt.id,
                statusStr[prevStageStatus]
          )
        }
        stage.set('status', FAILED, true);
        job.inc('stageIdxCounts.failed');

        ['num', 'running', 'succeeded', 'failed'].forEach(function(key) {
          var jobKey = ['taskIdxCounts', key].join('.');
          var resetValue =
                job.stageIDs.reduce(function(sum, stageId) {
                  return sum + (app.getStage(stageId).get('taskIdxCounts')[key] || 0);
                }, 0);
          l.info(
                "Resetting job %d 'taskIdxCounts.%s' on stage attempt %d.%d failure: %d -> %d",
                job.id, key, stage.id, attempt.id, job.get(jobKey), resetValue
          );
          job.set(jobKey, resetValue);
        });
        job.get('taskIdxCounts');
      }
    }

    stage.upsert();
    attempt.upsert();
    job.upsert();

  },

  SparkListenerTaskStart: function(app, e) {
    var stage = app.getStage(e);
    var job = app.getJobByStageId(stage.id);
    var stageAttempt = stage.getAttempt(e);

    var ti = e['Task Info'];
    var taskId = ti['Task ID'];

    var executor = app.getExecutor(ti);
    var stageExecutor = stageAttempt.getExecutor(ti).set({ host: executor.get('host'), port: executor.get('port') });

    var taskIndex = ti['Index'];
    var task = stageAttempt.getTask(taskIndex);
    var prevTaskStatus = task.get('status');

    var taskAttempt = stageAttempt.getTaskAttempt(taskId);
    var prevTaskAttemptStatus = taskAttempt.get('status');

    taskAttempt.fromTaskInfo(ti);

    if (prevTaskAttemptStatus) {
      var taskAttemptId = ti['Attempt'];
      l.error(
            "Unexpected TaskStart for %d (%s:%s), status: %s (%d) -> %s (%d)",
            taskId,
            stageAttempt.stageId + "." + stageAttempt.id,
            taskIndex + "." + taskAttemptId,
            statusStr[prevTaskAttemptStatus], prevTaskAttemptStatus,
            "RUNNING", RUNNING
      );
    } else {
      taskAttempt.set('status', RUNNING);
      job.inc('taskCounts.running');
      stageAttempt.inc('taskCounts.running');
      executor.inc('taskCounts.running').inc('taskCounts.num');
      stageExecutor.inc('taskCounts.running').inc('taskCounts.num');

      if (!prevTaskStatus) {
        task.set('status', RUNNING);
        stageAttempt.inc('taskIdxCounts.running');
        job.inc('taskIdxCounts.running');
      } else if (prevTaskStatus == FAILED) {
        task.set('status', RUNNING, true);
        stageAttempt.dec('taskIdxCounts.failed').inc('taskIdxCounts.running');
        job.dec('taskIdxCounts.failed').inc('taskIdxCounts.running');
      }
    }

    job.upsert();
    stageAttempt.upsert();
    stageExecutor.upsert();
    task.upsert();
    taskAttempt.upsert();
    executor.upsert();
  },

  SparkListenerTaskGettingResult: function(app, e) {
    var stageAttempt = stage.getAttempt(e);

    var ti = e['Task Info'];
    var taskId = ti['Task ID'];

    stageAttempt.getTaskAttempt(taskId).fromTaskInfo(ti).upsert();
  },

  SparkListenerTaskEnd: function(app, e) {
    var stage = app.getStage(e);
    var job = app.getJobByStageId(stage.id);
    var stageAttempt = stage.getAttempt(e);

    var ti = e['Task Info'];
    var taskId = ti['Task ID'];
    var taskIndex = ti['Index'];
    var taskAttemptId = ti['Attempt'];

    var executor = app.getExecutor(ti);
    var stageExecutor = stageAttempt.getExecutor(ti).set({ host: executor.get('host'), port: executor.get('port') });

    var task = stageAttempt.getTask(taskIndex).set({ type: e['Task Type'] });
    var prevTaskStatus = task.get('status');

    var taskAttempt = stageAttempt.getTaskAttempt(taskId).set({ end: removeKeySpaces(e['Task End Reason']) });
    var prevTaskAttemptStatus = taskAttempt.get('status');

    taskAttempt.fromTaskInfo(ti);

    var taskMetrics = maybeAddTotalShuffleReadBytes(removeKeySpaces(e['Task Metrics']));
    handleTaskMetrics(taskMetrics, app, job, stageAttempt, executor, stageExecutor, taskAttempt);
    handleBlockUpdates(taskMetrics, app, executor);

    var succeeded = !ti['Failed'];
    var status = succeeded ? SUCCEEDED : FAILED;
    var taskCountKey = succeeded ? 'taskCounts.succeeded' : 'taskCounts.failed';
    var taskIdxCountKey = succeeded ? 'taskIdxCounts.succeeded' : 'taskIdxCounts.failed';

    var prevNumFailed = task.get('failed') || 0;
    if (succeeded) {
      if (prevNumFailed) {
        stageAttempt.dec('failing.' + prevNumFailed);
        job.dec('failing.' + prevNumFailed);
      }
    } else {
      var numFailed = prevNumFailed + 1;
      task.inc('failed');
      if (prevNumFailed) {
        stageAttempt.dec('failed.' + prevNumFailed);
        job.dec('failed.' + prevNumFailed);
        if (task.get('status') != SUCCEEDED) {
          stageAttempt.dec('failing.' + prevNumFailed);
          job.dec('failing.' + prevNumFailed);
        }
      }
      stageAttempt.inc('failed.' + numFailed).inc('failing.' + numFailed);
      job.inc('failed.' + numFailed).inc('failing.' + numFailed);
    }

    if (prevTaskAttemptStatus == RUNNING) {
      taskAttempt.set('status', status, true);
      job.dec('taskCounts.running').inc(taskCountKey);
      stageAttempt.dec('taskCounts.running').inc(taskCountKey);
      executor.dec('taskCounts.running').inc(taskCountKey);
      stageExecutor.dec('taskCounts.running').inc(taskCountKey);

      if (!prevTaskStatus) {
        l.error(
              "Got TaskEnd for %d (%s:%s) with previous task status %s",
              taskId,
              stageAttempt.stageId + "." + stageAttempt.id,
              taskIndex + "." + taskAttemptId,
              statusStr[prevTaskStatus]
        );
      } else {
        if (prevTaskStatus == RUNNING) {
          task.set('status', status, true);
          stageAttempt.dec('taskIdxCounts.running').inc(taskIdxCountKey);
          job.dec('taskIdxCounts.running').inc(taskIdxCountKey);

        } else if (prevTaskStatus == FAILED) {
          if (succeeded) {
            task.set('status', status, true);
            stageAttempt.dec('taskIdxCounts.failed').inc('taskIdxCounts.succeeded');
            job.dec('taskIdxCounts.failed').inc('taskCount.succeeded');
          }
        } else {
          var logFn = succeeded ? l.info : l.warn;
          logFn(
                "Ignoring status %s for task %d (%s:%s) because existing status is SUCCEEDED",
                statusStr[status],
                taskId,
                stageAttempt.stageId + "." + stageAttempt.id,
                taskIndex + "." + taskAttemptId
          )
        }
      }
    } else {
      l.error(
            "Unexpected TaskEnd for %d (%s:%s), status: %s (%d) -> %s (%d)",
            taskId,
            stageAttempt.stageId + "." + stageAttempt.id,
            taskIndex + "." + taskAttemptId,
            statusStr[prevTaskAttemptStatus], prevTaskAttemptStatus,
            statusStr[status], status
      )
    }

    taskAttempt.upsert();
    task.upsert();
    stageAttempt.upsert();
    stageExecutor.upsert();
    executor.upsert();
    job.upsert();
    app.upsert();
  },

  SparkListenerEnvironmentUpdate: function(app, e) {
    colls.Environment.findOneAndUpdate(
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
  },
  SparkListenerBlockManagerAdded: function(app, e) {
    app
          .getExecutor(e)
          .set({
            maxMem: e['Maximum Memory'],
            'time.start': processTime(e['Timestamp']),
            host: e['Block Manager ID']['Host'],
            port: e['Block Manager ID']['Port']
          }, true)
          .upsert();
    app.inc('maxMem', e['Maximum Memory']).upsert();
  },
  SparkListenerBlockManagerRemoved: function(app, e) {
    var executor =
          app
                .getExecutor(e)
                .set({
                  'time.end': processTime(e['Timestamp']),
                  host: e['Block Manager ID']['Host'],
                  port: e['Block Manager ID']['Port']
                }, true)
                .upsert();
    app.dec('maxMem', executor.get('maxMem')).upsert();
  },

  SparkListenerUnpersistRDD: function(app, e) {
    var rddId = e['RDD ID'];
    var rdd = app.getRDD(rddId).set({ unpersisted: true });
    for (var eid in app.executors) {
      var executor = app.executors[eid];
      var rddExecutor = rdd.getExecutor(eid).set({ host: executor.get('host'), port: executor.get('port') });

      ['numBlocks', 'MemorySize', 'DiskSize', 'ExternalBlockStoreSize'].forEach(function(key) {
        var extant = rddExecutor.get(key) || 0;
        app.dec(key, extant);
        executor.dec(key, extant).upsert();
      });
      rddExecutor.set('unpersisted', true).upsert();
    }
    rdd.upsert();
    app.upsert();
  },

  SparkListenerExecutorAdded: function(app, e) {
    var ei = e['Executor Info'];
    app.getExecutor(e).set({
      'time.start': processTime(e['Timestamp']),
      host: ei['Host'],
      cores: ei['Total Cores'],
      urls: ei['Log Urls']
    }).upsert();
  },

  SparkListenerExecutorRemoved: function(app, e) {
    app
          .getExecutor(e)
          .set({
            'time.end': processTime(e['Timestamp']),
            reason: e['Removed Reason']
          })
          .upsert();
  },

  SparkListenerLogStart: function(app, e) {

  },
  SparkListenerExecutorMetricsUpdate: function(app, e) {
    var executor = app.getExecutor(e);
    l.debug("processing %d metrics updates..", e.Metrics.length);
    e.Metrics.map(function(m) {
      var stage = app.getStage(m);
      var job = app.getJob(stage.get('jobId'));
      var stageAttempt = stage.getAttempt(m);
      var taskAttempt = stageAttempt.getTaskAttempt(m);
      var stageExecutor = stageAttempt.getExecutor(e);
      var taskMetrics = maybeAddTotalShuffleReadBytes(removeKeySpaces(m['Task Metrics']));
      handleTaskMetrics(taskMetrics, app, job, stageAttempt, executor, stageExecutor, taskAttempt);
      taskAttempt.upsert();
      stageAttempt.upsert();
      executor.upsert();
      stageExecutor.upsert();
      job.upsert();
      app.upsert();
    });
  }
};

function handleEvent(e) {
  l.debug('Got data: ', e);
  if ('Event' in e) {
    getApp(e, function(app) {
      handlers[e['Event']](app, e);
    });
  }
}

colls.init(mongoUrl, function(db) {
  var server = net.createServer(function(c) {
    l.warn("client connected");
    var setupOboe = function() {
      l.debug("Registering oboe");
      oboe(c).node('!', function(e) {
        handleEvent(e);
      }).fail(function(e) {
        throw e.thrown;
      });
    };
    setupOboe();

    c.on('end', function() {
      l.warn("client disconnected");
    })
  });
  server.listen(port, function() {
    l.warn("Server listening on: http://localhost:%s", port);
  });
});
