
var RddBlock = require('./block').RddBlock;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function RDDExecutor(rdd, executor) {
  this.appId = rdd.appId;
  this.rddId = rdd.id;
  this.execId = executor.id;

  this.rdd = rdd;
  this.executor = executor;

  this.blocks = {};

  this.init(
        [ 'appId', 'rddId', 'execId' ],
        {
          numCachedPartitions: { sums: [ rdd, executor, executor.app ] }
        }
  );
}

RDDExecutor.prototype.getBlock = function(blockIndex) {
  if (!(blockIndex in this.blocks)) {
    this.blocks[blockIndex] = new RddBlock(this, blockIndex);
  }
  return this.blocks[blockIndex];
};

RDDExecutor.prototype.remove = function(unpersist) {
  if (unpersist) {
    this.set('unpersisted', true);
  }
  for (var blockId in this.blocks) {
    var block = this.blocks[blockId];
    block.remove();
  }
};

mixinMongoMethods(RDDExecutor, 'RDDExecutor', 'RDDExecutors');

module.exports.RDDExecutor = RDDExecutor;
