
var objUtils = require('../utils/objs');
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;
var removeKeySpaces = objUtils.removeKeySpaces;
var RddBlock = require('./block').RddBlock;

function RDD(appId, id) {
  this.appId = appId;
  this.id = id;
  this.findObj = { appId: appId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;

  this.applyRateLimit = true;

  this.blocks = {};
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

module.exports.RDD = RDD;
