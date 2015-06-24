
var utils = require('../utils');
var mixinMongoMethods = utils.mixinMongoMethods;
var removeKeySpaces = utils.removeKeySpaces;

function RDD(appId, id) {
  this.appId = appId;
  this.id = id;
  this.findObj = { appId: appId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;
  this.key = [ 'app', appId, 'rdd', id ].join('-');
  this.applyRateLimit = true;
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

module.exports.RDD = RDD;
