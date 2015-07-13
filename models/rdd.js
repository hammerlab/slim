
var objUtils = require('../utils/objs');
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;
var removeKeySpaces = objUtils.removeKeySpaces;
var RddBlock = require('./block').RddBlock;
var RDDExecutor = require('./rdd-executor').RDDExecutor;
var getExecutorId = require('./executor').getExecutorId;

function RDD(appId, id) {
  this.appId = appId;
  this.id = id;

  this.init([ 'appId', 'id' ]);

  this.blocks = {};
  this.executors = {};
}

mixinMongoMethods(RDD, "RDD", "RDDs");

RDD.prototype.fromRDDInfo = function(ri) {
  return this.set({
    name: ri['Name'],
    parentIDs: ri['Parent IDs'],
    StorageLevel: removeKeySpaces(ri['Storage Level']),
    numPartitions: ri['Number of Partitions'],
    numCachedPartitions: ri['Number of Cached Partitions'],
    MemorySize: ri['MemorySize'],
    ExternalBlockStoreSize: ri['ExternalBlockStore Size'],
    DiskSize: ri['Disk Size'],
    scope: ri['Scope']
  });
};

RDD.prototype.getBlock = function(blockIndex) {
  if (!(blockIndex in this.blocks)) {
    this.blocks[blockIndex] = new RddBlock(this, blockIndex);
  }
  return this.blocks[blockIndex];
};

RDD.prototype.getExecutor = function(execId) {
  execId = getExecutorId(execId);
  if (!(execId in this.executors)) {
    this.executors[execId] = new RDDExecutor(this, execId);
  }
  return this.executors[execId];
};

module.exports.RDD = RDD;
