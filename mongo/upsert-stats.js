
var l = require('../utils/log').l;

var UpsertStats = function() {
  this.started = 0;
  this.ended = 0;
  this.inFlight = 0;

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
  this.dec = function() {
    this.ended++;
    this.inFlight--;
    this.maybeWarn();
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
