
var RddBlock = require('./block').RddBlock;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function RDDExecutor(rdd, executor) {
  this.appId = rdd.appId;
  this.rddId = rdd.id;
  this.execId = executor.id;

  var callbackObj = {};
  ['MemorySize', 'DiskSize', 'ExternalBlockStoreSize', 'numBlocks'].forEach((key) => {
    callbackObj[key] = { sums: [ rdd.app, rdd, executor ] };
  });

  this.blocks = {};

  this.init(
        [ 'appId', 'rddId', 'execId' ],
        callbackObj
  );
}

RDDExecutor.prototype.getBlock = function(blockIndex) {
  if (!(blockIndex in this.blocks)) {
    this.blocks[blockIndex] = new RddBlock(this, blockIndex);
  }
  return this.blocks[blockIndex];
};

mixinMongoMethods(RDDExecutor, 'RDDExecutor', 'RDDExecutors');

module.exports.RDDExecutor = RDDExecutor;
