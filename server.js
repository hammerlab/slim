
var http = require('http');
var extend = require('node.extend');

var url = 'mongodb://localhost:27017/spruit';

var getApp = require('./models/app').getApp;
var colls = require('./collections');

var utils = require("./utils");
var statusStr = utils.status;

var l = require('./log').l;

var PENDING = utils.PENDING;
var RUNNING = utils.RUNNING;
var FAILED = utils.FAILED;
var SUCCEEDED = utils.SUCCEEDED;
var SKIPPED = utils.SKIPPED;

function toSeq(m) {
  var ret = [];
  for (k in m) {
    ret.push([k, m[k]]);
  }
  return ret;
}

var handlers = {

  SparkListenerApplicationStart: function(e) {
    getApp(e['appId']).fromEvent(e).upsert();
  },

  SparkListenerApplicationEnd: function(e) {
    var app = getApp(e);
    app.set('time.end', app.processTime(e['Timestamp'])).upsert();
  },

  SparkListenerJobStart: function(e) {
    var app = getApp(e);
    var job = app.getJob(e);
    var numTasks = 0;

    var stageInfos = e['Stage Infos'];

    stageInfos.forEach(function(si) {

      var stage = app.getStage(si['Stage ID']).fromStageInfo(si).set('jobId', job.id).upsert();
      app.stageIDstoJobIDs[si['Stage ID']] = job.id;

      var attempt = stage.getAttempt(si['Stage Attempt ID']).fromStageInfo(si).upsert();

      si['RDD Info'].forEach(function(ri) {
        app.getRDD(ri['RDD ID']).fromRDDInfo(ri).upsert();
      }.bind(this));

      numTasks += si['Number of Tasks'];
    });

    job.set({
      'time.start': job.processTime(e['Submission Time']),
      stageIDs: e['Stage IDs'],
      'taskCounts.num': numTasks,
      'stageCounts.num': e['Stage IDs'].length,
      properties: e['Properties']
    }).upsert();

  },

  SparkListenerJobEnd: function(e) {
    var app = getApp(e);
    var job = app.getJob(e);

    job.set({
      'time.end': job.processTime(e['Completion Time']),
      result: e['Job Result'],
      succeeded: e['Job Result']['Result'] == 'JobSucceeded',
      ended: true
    }).upsert();

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

  SparkListenerStageSubmitted: function(e) {
    var app = getApp(e);
    var si = e['Stage Info'];

    var stage = app.getStage(si);
    var attempt = stage.getAttempt(si);
    var prevStatus = attempt.get('status');
    if (prevStatus) {
      l.err(
            "Stage " + id + " marking attempt " + attempt.id + " as RUNNING despite extant status " + prevStatus
      );
    }

    // Crashes if extant status found.
    attempt.fromStageInfo(si).set({ started: true, status: RUNNING }).upsert();

    app.getJobByStageId(stage.id).inc('stageCounts.running').upsert();

    stage.fromStageInfo(si).set({ properties: e['Properties'] }).inc('attempts.num').inc('attempts.running').upsert();
  },

  SparkListenerStageCompleted: function(e) {
    var app = getApp(e);
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
      l.info("before dec: " + job.get('stageCounts.running'));
      job.dec('stageCounts.running');
      l.info("after dec: " + job.get('stageCounts.running'));
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

  SparkListenerTaskStart: function(e) {
    var app = getApp(e);
    var stage = app.getStage(e);
    var stageAttempt = stage.getAttempt(e);

    var ti = e['Task Info'];
    var taskId = ti['Task ID'];

    var taskIndex = ti['Index'];
    var task = stage.getTask(taskIndex);
    var prevTaskStatus = task.get('status');

    var taskAttempt = stageAttempt.getTaskAttempt(taskId);
    var prevTaskAttemptStatus = task.get('status');

    taskAttempt.fromTaskInfo(ti);

    if (prevTaskAttemptStatus) {
      var taskAttemptId = ti['Attempt'];
      l.error(
            "Found extant status %s (%d) for task %d (%s:%s)",
            statusStr[prevTaskAttemptStatus], prevTaskAttemptStatus,
            taskId,
            stage.id + "." + stageAttempt.id,
            taskIndex + "." + taskAttemptId
      );
    } else {
      taskAttempt.set('status', RUNNING);
      stageAttempt.inc('taskCounts.running');

      if (!prevTaskStatus) {
        task.set('status', RUNNING);
        stage.inc('taskCounts.running');
      } else if (prevTaskStatus == FAILED) {
        task.set('status', RUNNING, true);
        stage.dec('taskCounts.failed').inc('taskCounts.running');
      }
    }

    stage.upsert();
    stageAttempt.upsert();
    task.upsert();
    taskAttempt.upsert();

  },

  SparkListenerTaskGettingResult: function(e) {
    var app = getApp(e);
    var stageAttempt = stage.getAttempt(e);

    var ti = e['Task Info'];
    var taskId = ti['Task ID'];

    stageAttempt.getTaskAttempt(taskId).fromTaskInfo(ti).upsert();
  },

  SparkListenerTaskEnd: function(e) {
    var app = getApp(e);
    var stage = app.getStage(e);
    var stageAttempt = stage.getAttempt(e);

    var ti = e['Task Info'];
    var taskId = ti['Task ID'];
    var taskIndex = ti['Index'];
    var taskAttemptId = ti['Attempt'];

    var task = stage.getTask(taskIndex);
    var prevTaskStatus = task.get('status');

    var taskAttempt = stageAttempt.getTaskAttempt(taskId);
    var prevTaskAttemptStatus = task.get('status');

    taskAttempt.fromTaskInfo(ti);
    var succeeded = !ti['Failed'];
    var status = succeeded ? SUCCEEDED : FAILED;
    var taskCountKey = succeeded ? 'taskCounts.succeeded' : 'taskCounts.failed';

    if (prevTaskAttemptStatus == RUNNING) {
      taskAttempt.set('status', status, true);
      stageAttempt.dec('taskCounts.running').inc(taskCountKey);

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

        } else if (prevTaskStatus == FAILED) {
          if (succeeded) {
            task.set('status', status, true);
            stage.dec('taskCounts.failed').inc('taskCount.succeeded');
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
            "Got TaskEnd for %d (%s:%s) with previous status %s (%d)",
            taskId,
            stage.id + "." + stageAttempt.id,
            taskIndex + "." + taskAttemptId,
            statusStr[prevTaskAttemptStatus], prevTaskAttemptStatus
      )
    }

    stage.upsert();
    stageAttempt.upsert();
    task.upsert();
    taskAttempt.upsert();
  },

  SparkListenerEnvironmentUpdate: function(e) {
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
          utils.upsertOpts,
          utils.upsertCb("Environment")
    );
  },
  SparkListenerBlockManagerAdded: function(e) {
    var app = getApp(e);
    app.getExecutor(e).set({
      maxMem: e['Maximum Memory'],
      'time.start': app.processTime(e['Timestamp']),
      host: e['Block Manager ID']['Host'],
      port: e['Block Manager ID']['Port']
    }).upsert();
  },
  SparkListenerBlockManagerRemoved: function(e) {
    var app = getApp(e);
    app.getExecutor(e).set({
      'time.end': app.processTime(e['Timestamp']),
      host: e['Block Manager ID']['Host'],
      port: e['Block Manager ID']['Port']
    }).upsert();
  },
  SparkListenerUnpersistRDD: function(e) {
  },

  SparkListenerExecutorAdded: function(e) {
    var app = getApp(e);
    var ei = e['Executor Info'];
    app.getExecutor(e).set({
      'time.start': app.processTime(e['Timestamp']),
      host: ei['Host'],
      cores: ei['Total Cores'],
      urls: ei['Log Urls']
    }).upsert();
  },

  SparkListenerExecutorRemoved: function(e) {
    var app = getApp(e);
    app.getExecutor(e).set({
      'time.end': app.processTime(e['Timestamp']),
      reason: e['Removed Reason']
    }).upsert();
  },

  SparkListenerLogStart: function(e) {

  },
  SparkListenerExecutorMetricsUpdate: function(e) {

  }
};

//We need a function which handles requests and send response
function handleRequest(request, response) {
  var d = '';
  request.on('data', function(chunk) {
    d += chunk;
  });
  request.on('end', function() {
    if (d) {
      var e = JSON.parse(d);
      l.info('Got data: ' + d);
      handlers[e['Event']](e)
    }
    response.end('OK');
  });
}

const SPARK_LISTENER_PORT=8123;

colls.init(url, function(db) {
  var server = http.createServer(handleRequest);

  server.listen(SPARK_LISTENER_PORT, function() {
    //Callback triggered when server is successfully listening. Hurray!
    l.info("Server listening on: http://localhost:%s", SPARK_LISTENER_PORT);
  });
});
