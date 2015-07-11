
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

var handlers = {

  SparkListenerApplicationStart: function(app, e) {
    app.fromEvent(e).upsert();
  },

  SparkListenerApplicationEnd: function(app, e) {
    app.set('time.end', processTime(e['Timestamp'])).setDuration().upsert();
  },

  SparkListenerJobStart: function(app, e) {
    var job = app.getJob(e);
    var numTasks = 0;

    var stageInfos = e['Stage Infos'];

    stageInfos.forEach(function(si) {

      var stage = app.getStage(si['Stage ID']).fromStageInfo(si).set('jobId', job.id).upsert();
      app.stageIDstoJobIDs[si['Stage ID']] = job.id;

      var attempt = stage.getAttempt(si['Stage Attempt ID']).fromStageInfo(si).upsert();

      si['RDD Info'].forEach(function(ri) {
        app.getRDD(ri).fromRDDInfo(ri).upsert();
      }.bind(this));

      numTasks += si['Number of Tasks'];
    });

    job.set({
      'time.start': processTime(e['Submission Time']),
      stageIDs: e['Stage IDs'],
      'taskCounts.num': numTasks,
      'stageCounts.num': e['Stage IDs'].length,
      properties: e['Properties']
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
          .setDuration()
          .upsert();

    job.get('stageIDs').map(function(sid) {
      var stage = app.getStage(sid);
      var status = stage.get('status');
      if (status == RUNNING || status == FAILED) {
        l.err("Found unexpected status " + status + " for stage " + stage.id + " when marking job " + job.id + " complete.");
      } else if (!status) {
        // Will fail if a status exists for this stage
        stage.set('status', SKIPPED).upsert();
      }
    });
  },

  SparkListenerStageSubmitted: function(app, e) {
    var si = e['Stage Info'];

    var stage = app.getStage(si);
    var attempt = stage.getAttempt(si);
    var prevStatus = attempt.get('status');
    if (prevStatus) {
      l.err(
            "Stage " + stage.id + " marking attempt " + attempt.id + " as RUNNING despite extant status " + prevStatus
      );
    }

    attempt.fromStageInfo(si).set({ started: true, status: RUNNING }).upsert();

    app.getJobByStageId(stage.id).inc('stageCounts.running').upsert();

    stage.fromStageInfo(si).set({ properties: e['Properties'] }).inc('attempts.num').inc('attempts.running').upsert();
  },

  SparkListenerStageCompleted: function(app, e) {
    var si = e['Stage Info'];

    var stage = app.getStage(si);
    stage.fromStageInfo(si);
    var prevStageStatus = stage.get('status');

    var attempt = stage.getAttempt(si);

    var prevAttemptStatus = attempt.get('status');
    var newAttemptStatus = si['Failure Reason'] ? FAILED : SUCCEEDED;

    attempt.fromStageInfo(si).set({ ended: true }).set('status', newAttemptStatus, true).upsert();

    var job = app.getJobByStageId(stage.id);

    if (prevAttemptStatus == RUNNING) {
      stage.dec('attempts.running');
      job.dec('stageCounts.running');
    } else {
      l.err(
            "Got status " + newAttemptStatus + " for stage " + stage.id + " attempt " + attempt.id + " with existing status " + prevAttemptStatus
      );
    }
    if (newAttemptStatus == SUCCEEDED) {
      if (prevStageStatus == SUCCEEDED) {
        l.info("Ignoring attempt " + attempt.id + " SUCCEEDED in stage " + stage.id + " that is already SUCCEEDED");
      } else {
        stage.set('status', newAttemptStatus, true).inc('attempts.succeeded');
        job.inc('stageCounts.succeeded');
      }
    } else {
      // FAILED
      if (prevStageStatus == SUCCEEDED) {
        l.info("Ignoring attempt " + attempt.id + " FAILED in stage " + stage.id + " that is already SUCCEEDED");
      } else {
        stage.set('status', newAttemptStatus, true).inc('attempts.failed');
        job.inc('stageCounts.failed');
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
    var executorStageKey = ['stages', stage.id, stageAttempt.id, 'taskCounts', ''].join('.');

    var taskIndex = ti['Index'];
    var task = stage.getTask(taskIndex);
    var prevTaskStatus = task.get('status');

    var taskAttempt = stageAttempt.getTaskAttempt(taskId);
    var prevTaskAttemptStatus = taskAttempt.get('status');

    taskAttempt.fromTaskInfo(ti);

    if (prevTaskAttemptStatus) {
      var taskAttemptId = ti['Attempt'];
      l.error(
            "Unexpected TaskStart for %d (%s:%s), status: %s (%d) -> %s (%d)",
            taskId,
            stage.id + "." + stageAttempt.id,
            taskIndex + "." + taskAttemptId,
            statusStr[prevTaskAttemptStatus], prevTaskAttemptStatus,
            "RUNNING", RUNNING
      );
    } else {
      taskAttempt.set('status', RUNNING);
      stageAttempt.inc('taskCounts.running');
      executor.inc('taskCounts.running').inc('taskCounts.num').inc(executorStageKey + 'running').inc(executorStageKey + 'num');

      if (!prevTaskStatus) {
        task.set('status', RUNNING);
        stage.inc('taskCounts.running');
        job.inc('taskCounts.running');
      } else if (prevTaskStatus == FAILED) {
        task.set('status', RUNNING, true);
        stage.dec('taskCounts.failed').inc('taskCounts.running');
        job.dec('taskCounts.failed').inc('taskCounts.running');
      }
    }

    job.upsert();
    stage.upsert();
    stageAttempt.upsert();
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
    var executorStageKey = 'stages.' + stage.id + '.' + stageAttempt.id + '.';

    var task = stage.getTask(taskIndex).set({ type: e['Task Type'] });
    var prevTaskStatus = task.get('status');
    var prevTaskMetrics = task.get('metrics');

    var taskAttempt = stageAttempt.getTaskAttempt(taskId).set({ end: removeKeySpaces(e['Task End Reason']) });
    var prevTaskAttemptStatus = taskAttempt.get('status');

    var taskMetrics = maybeAddTotalShuffleReadBytes(removeKeySpaces(e['Task Metrics']));
    taskAttempt.fromTaskInfo(ti);
    var prevTaskAttemptMetrics = taskAttempt.get('metrics');
    var newTaskAttemptMetrics = taskMetrics;

    taskAttempt.set('metrics', newTaskAttemptMetrics);

    var taskAttemptMetricsDiff = subObjs(newTaskAttemptMetrics, prevTaskAttemptMetrics);
    executor.set("metrics", addObjs(executor.get('metrics'), taskAttemptMetricsDiff), true);
    executor.set(executorStageKey + "metrics", addObjs(executor.get(executorStageKey + 'metrics'), taskAttemptMetricsDiff), true);
    stageAttempt.set("metrics", addObjs(stageAttempt.get('metrics'), taskAttemptMetricsDiff), true);
    job.set("metrics", addObjs(job.get("metrics"), taskAttemptMetricsDiff), true);

    var newTaskMetrics = maxObjs(prevTaskMetrics, newTaskAttemptMetrics);
    var taskMetricsDiff = subObjs(newTaskMetrics, prevTaskMetrics);
    task.set("metrics", newTaskMetrics, true);
    stage.set("metrics", addObjs(stage.get("metrics"), taskMetricsDiff), true);

    var updatedBlocks = taskMetrics['UpdatedBlocks'];
    var rdds = [];
    var blocks = [];
    if (updatedBlocks) {
      updatedBlocks.forEach(function (blockInfo) {
        var blockId = blockInfo['BlockID'];

        var rddIdMatch = blockId.match(/^rdd_([0-9]+)_([0-9]+)$/);
        var rdd = null;
        var block = null;
        var blockWasCached = false;
        var rddKey = null;
        if (rddIdMatch) {
          var rddId = parseInt(rddIdMatch[1]);
          var blockIndex = parseInt(rddIdMatch[2]);
          rddKey = ['blocks', 'rdd', rddId].join('.');

          rdd = app.getRDD(rddId);
          rdds.push(rdd);

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
          executor.inc(key, delta).inc(rddKey + '.' + key, delta);
          app.inc(key, delta);
          if (rdd) {
            rdd.inc(key, delta);
          }
          block.set(key, status[key], true);
        });
        if (!blockIsCached) {
          if (blockWasCached) {
            executor.dec('numBlocks').dec(rddKey + '.numBlocks');
            if (rdd) rdd.dec("numCachedPartitions")
          }
        } else {
          if (!blockWasCached) {
            executor.inc('numBlocks').inc(rddKey + '.numBlocks');
            if (rdd) rdd.inc("numCachedPartitions")
          }
        }
        if (rdd) {
          block.set('StorageLevel', status['StorageLevel'], true);
        }
        block.set({ host: executor.get('host'), port: executor.get('port') }, true);
      });
    }

    var succeeded = !ti['Failed'];
    var status = succeeded ? SUCCEEDED : FAILED;
    var taskCountKey = succeeded ? 'taskCounts.succeeded' : 'taskCounts.failed';

    if (prevTaskAttemptStatus == RUNNING) {
      taskAttempt.set('status', status, true);
      stageAttempt.dec('taskCounts.running').inc(taskCountKey);
      executor.dec('taskCounts.running').inc(taskCountKey).dec(executorStageKey + 'taskCounts.running').inc(executorStageKey + taskCountKey);

      if (!prevTaskStatus) {
        l.error(
              "Got TaskEnd for %d (%s:%s) with previous task status %s",
              taskId,
              stage.id + "." + stageAttempt.id,
              taskIndex + "." + taskAttemptId,
              statusStr[prevTaskStatus]
        );
      } else {
        if (prevTaskStatus == RUNNING) {
          task.set('status', status, true);
          stage.dec('taskCounts.running').inc(taskCountKey);
          job.dec('taskCounts.running').inc(taskCountKey);

        } else if (prevTaskStatus == FAILED) {
          if (succeeded) {
            task.set('status', status, true);
            stage.dec('taskCounts.failed').inc('taskCount.succeeded');
            job.dec('taskCounts.failed').inc('taskCount.succeeded');
          }
        } else {
          var logFn = succeeded ? l.info : l.warn;
          logFn(
                "Ignoring status %s for task %d (%s:%s) because existing status is SUCCEEDED",
                statusStr[status],
                taskId,
                stage.id + "." + stageAttempt.id,
                taskIndex + "." + taskAttemptId
          )
        }
      }
    } else {
      l.error(
            "Unexpected TaskEnd for %d (%s:%s), status: %s (%d) -> %s (%d)",
            taskId,
            stage.id + "." + stageAttempt.id,
            taskIndex + "." + taskAttemptId,
            statusStr[prevTaskAttemptStatus], prevTaskAttemptStatus,
            statusStr[status], status
      )
    }

    stage.upsert();
    stageAttempt.upsert();
    task.upsert();
    taskAttempt.upsert();
    executor.upsert();
    job.upsert();
    app.upsert();
    rdds.forEach(function(rdd) { rdd.upsert(); });
    blocks.forEach(function(block) { block.upsert(); });
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
                .setDuration()
                .upsert();
    app.dec('maxMem', executor.get('maxMem')).upsert();
  },

  SparkListenerUnpersistRDD: function(app, e) {
    var rddId = e['RDD ID'];
    app.getRDD(rddId).set({ unpersisted: true }).upsert();
    for (var eid in app.executors) {
      var executor = app.executors[eid];
      var rddKey = ['blocks', 'rdd', rddId].join('.');
      app
            .dec('numBlocks', executor.get(rddKey + '.numBlocks') || 0)
            .dec('MemorySize', executor.get(rddKey + '.MemorySize') || 0)
            .dec('DiskSize', executor.get(rddKey + '.DiskSize') || 0)
            .dec('ExternalBlockStoreSize', executor.get(rddKey + '.ExternalBlockStoreSize') || 0);
      executor
            .dec('numBlocks', executor.get(rddKey + '.numBlocks') || 0)
            .dec('MemorySize', executor.get(rddKey + '.MemorySize') || 0)
            .dec('DiskSize', executor.get(rddKey + '.DiskSize') || 0)
            .dec('ExternalBlockStoreSize', executor.get(rddKey + '.ExternalBlockStoreSize') || 0)
            .unset(rddKey)
            .upsert();
    }
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
          .setDuration()
          .upsert();
  },

  SparkListenerLogStart: function(app, e) {

  },
  SparkListenerExecutorMetricsUpdate: function(app, e) {

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
