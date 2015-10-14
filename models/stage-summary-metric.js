
var argv = require('minimist')(process.argv.slice(2));

var l = require("../utils/log").l;

var apps = null;
var utils = require("../utils/utils");
var acc = utils.acc;
var processTime = utils.processTime;
var accumulablesObj = utils.accumulablesObj;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

var OST = require("../utils/ost").OST;

function StageSummaryMetric(stageAttempt, id) {
  this.app = stageAttempt.app;
  this.stageAttempt = stageAttempt;

  this.appId = stageAttempt.appId;
  this.stageId = stageAttempt.stageId;
  this.stageAttemptId = stageAttempt.id;
  this.id = id;

  this.fn = acc(id);

  this.init([ 'appId', 'stageId', 'stageAttemptId', 'id' ]);

  this.tree = new OST();
}

StageSummaryMetric.prototype.initTree = function() {
  for (var k in this.stageAttempt.task_attempts) {
    var t = this.stageAttempt.task_attempts[k];
    this.handleMetrics(undefined, t.get('metrics'));
  }
};

StageSummaryMetric.prototype.handleValueChange = function(prevValue, newValue) {
  if (newValue !== undefined && newValue !== null) {
    if (prevValue !== undefined) {
      if (prevValue !== newValue) {
        var n = this.tree.search(prevValue);
        if (!n) {
          l.error(
                "%s: didn't find node for value %s in tree of size %d:\n\t%s",
                this.id,
                prevValue,
                this.tree.size(),
                this.tree.str("\n\t")
          );
        } else {
          this.tree.delete(n);
        }
      }
    }
    if (prevValue !== newValue) {
      this.tree.insert(newValue, newValue);
    }
  }
};

StageSummaryMetric.prototype.handleMetrics = function(prevTask, newTask) {
  this.handleValueChange(this.fn(prevTask), this.fn(newTask));
};

function makeStatsArr(n) {
  return [
    ['min', 0],
    ['tf', parseInt(n/4)],
    ['median', parseInt(n/2)],
    ['sf', parseInt(3*n/4)],
    ['max', Math.max(0, n - 1)]
  ];
}

StageSummaryMetric.prototype.syncChanges = function() {
  makeStatsArr(this.tree.size()).forEach(function(stat) {
    var node = this.tree.select(stat[1]);
    if (node && node.value !== undefined && node.value !== null) {
      this.set(stat[0], node.value, true);
    }
  }.bind(this));
};

mixinMongoMethods(StageSummaryMetric, "StageSummaryMetric", "StageSummaryMetrics");

module.exports.StageSummaryMetric = StageSummaryMetric;
