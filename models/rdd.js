
var objUtils = require('../utils/objs');
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;
var removeKeySpaces = objUtils.removeKeySpaces;
var RDDExecutor = require('./rdd-executor').RDDExecutor;
var getExecutorId = require('./executor').getExecutorId;
var isEmptyObject = require('../utils/utils').isEmptyObject;

var REMOVED = require('../utils/utils').REMOVED;

function RDD(app, id) {
  this.appId = app.id;
  this.id = id;

  this.init([ 'appId', 'id' ]);

  this.blocks = {};
  this.executors = {};

  this.upsertHooks.push(this.updateFractionCached);
}

mixinMongoMethods(RDD, "RDD", "RDDs", 5);

RDD.prototype.fromRDDInfo = function(ri) {
  return this.set({
    name: ri['Name'],
    parentIDs: ri['Parent IDs'],
    numPartitions: ri['Number of Partitions'],
    scope: ri['Scope']
  }).set(
        {
          StorageLevel: removeKeySpaces(ri['Storage Level']),
          MemorySize: ri['MemorySize'],
          ExternalBlockStoreSize: ri['ExternalBlockStore Size'],
          DiskSize: ri['Disk Size'],
          numCachedPartitions: ri['Number of Cached Partitions']
        },
        true
  );
};

RDD.prototype.getExecutor = function(executor) {
  execId = executor.id;
  if (!(execId in this.executors)) {
    this.executors[execId] = new RDDExecutor(this, executor);
  }
  return this.executors[execId];
};

RDD.prototype.removeExecutor = function(executor) {
  if (executor.id in this.executors) {
    this.executors[executor.id].remove();
    delete this.executors[executor.id];
  }
};

RDD.prototype.unpersist = function() {
  this.set({ unpersisted: true });
  for (var eid in this.executors) {
    var executor = this.executors[eid];
    executor.set({ host: executor.get('host'), port: executor.get('port') }).remove(true);
  }
};

RDD.prototype.updateFractionCached = function() {
  var numCachedPartitions = this.get('numCachedPartitions');
  var numPartitions = this.get('numPartitions');
  if (numCachedPartitions && numPartitions) {
    this.set('fractionCached', this.get('numCachedPartitions') / this.get('numPartitions'), true);
  }
  return this;
};

module.exports.RDD = RDD;
