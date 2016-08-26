
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function ExecutorThreadDump(appId, execId, threadId) {
  this.appId = appId;
  this.execId = execId;
  this.id = threadId;

  this.init(['appId', 'execId', 'id']);
}

mixinMongoMethods(ExecutorThreadDump, "ExecutorThreadDump", "ExecutorThreadDumps");

ExecutorThreadDump.prototype.fromThreadInfo = function (ti) {
  return this.set({
    'threadName': ti['threadName'],
    'threadState': ti['threadState'],
    'stackTrace': ti['stackTrace']
  }, allowExtant = true);
};

module.exports.ExecutorThreadDump = ExecutorThreadDump;
