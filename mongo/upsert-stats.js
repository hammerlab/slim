
var l = require('../utils/log').l;
var moment = require('moment');

var UpsertStats = function() {
  this.started = 0;
  this.ended = 0;
  this.inFlight = 0;
  this.totalTime = 0;

  var thresholds = [0, 100, 200, 500];
  var thresholdMinIdx = 0;
  var thresholdMaxIdx = 1;
  var lastWarned = 0;

  var min = thresholds[thresholdMinIdx];
  var max = thresholds[thresholdMaxIdx];

  this.inc = function() {
    this.started++;
    this.inFlight++;
    this.maybeWarn();
  };

  this.dec = function(before) {
    this.ended++;
    this.inFlight--;
    this.totalTime += moment() - before;
    this.maybeWarn();
  };

  var lastLog = {
    t: moment(),
    started: 0,
    ended: 0,
    totalTime: 0
  };

  this.logStatus = function(queue, numBlocked) {
    var now = moment();
    var startDelta = this.started - lastLog.started;
    var endDelta = this.ended - lastLog.ended;
    var timeDelta = (now - lastLog.t) / 1000;
    var totalTimeDelta = this.totalTime - lastLog.totalTime;
    if (startDelta || endDelta) {
      l.info(
            "In flight: %d, blocked: %d, queue: %d. Last %ss: +%d,-%d (+%s,-%s/s), %dms avg. Total +%d,-%d",
            this.inFlight, numBlocked, queue.size(),
            timeDelta.toFixed(1),
            startDelta, endDelta,
            (startDelta / timeDelta).toFixed(1), (endDelta / timeDelta).toFixed(1),
            parseInt(totalTimeDelta / endDelta),
            this.started, this.ended
      );
    }
    lastLog = {
      t: now,
      started: this.started,
      ended: this.ended,
      totalTime: this.totalTime
    }
  };

  this.maybeWarn = function() {
    var d = this.inFlight;
    if (d == max || d == min) {
      if (d != lastWarned) {
        l.warn("Upsert backlog: %d (%d started, %d finished)", d, this.started, this.ended);
        lastWarned = d;
      }
      if (d == max) {
        if (thresholdMaxIdx == thresholds.length - 1) {
          thresholds.push(thresholds[thresholds.length - 3] * 10);
          thresholds.push(thresholds[thresholds.length - 3] * 10);
          thresholds.push(thresholds[thresholds.length - 3] * 10);
        }
        thresholdMaxIdx++;
        thresholdMinIdx = thresholdMaxIdx - 2;
      } else {
        if (thresholdMinIdx == 0) {
          thresholdMaxIdx = 1;
        } else {
          thresholdMinIdx--;
          thresholdMaxIdx = thresholdMinIdx + 2;
        }
      }
      min = thresholds[thresholdMinIdx];
      max = thresholds[thresholdMaxIdx];
    }
  };
};

module.exports.UpsertStats = UpsertStats;
