
var objUtils = require('../utils/objs');
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;
var removeKeySpaces = objUtils.removeKeySpaces;
var RddBlock = require('./block').RddBlock;
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

  this.upsertHooks = [ this.updateFractionCached ];
}

mixinMongoMethods(RDD, "RDD", "RDDs");

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

RDD.prototype.updateFractionCached = function() {
  if (this.get('numCachedPartitions') && this.get('numPartitions')) {
    this.set('fractionCached', this.get('numCachedPartitions') / this.get('numPartitions'), true);
  }
  return this;
};

RDD.prototype.handleExecutorRemoved = function(execId) {
  execId = getExecutorId(execId);
  var executor = null;
  if (execId in this.executors) {
    executor = this.executors[execId];
    ['MemorySize', 'DiskSize', 'ExternalBlockStoreSize'].forEach(function (key) {
      if (executor.get(key)) {
        this.dec(key, executor.get(key));
      }
    }.bind(this));
    this.dec('numCachedPartitions', executor.get('numBlocks'));
    executor.set('status', REMOVED, true);
  }
  for (var blockId in this.blocks) {
    var block = this.blocks[blockId];
    var execIds = block.get('execIds');
    if (execIds && execIds.indexOf(execId) >= 0) {
      block.pull('execIds', execId);
      if (isEmptyObject(block.get('execIds'))) {
        block.set('status', REMOVED, true);
      } else {
        if (block.get('execId') == execId) {
          var newExecId = null;
          for (newExecId in block.get('execIds')) {
            break;
          }
          block.set('execId', newExecId, true);
        }
      }
      block.upsert();
    }
  }
  return executor;
};

module.exports.RDD = RDD;
