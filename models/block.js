
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;


function RddBlock(rdd, id) {
  this.appId = rdd.appId;
  this.rddId = rdd.id;
  this.id = id;

  this.init(['appId', 'rddId', 'id']);
}

RddBlock.prototype.isCached = function() {
  return this.get("MemorySize") || this.get("DiskSize") || this.get("ExternalBlockStoreSize");
};

mixinMongoMethods(RddBlock, "RddBlock", "RddBlocks");


function NonRddBlock(executor, id) {
  this.appId = executor.appId;
  this.execId = executor.id;
  this.id = id;

  this.init([ 'appId', 'execId', 'id' ]);
}

mixinMongoMethods(NonRddBlock, "NonRddBlock", "NonRddBlocks");


module.exports.RddBlock = RddBlock;
module.exports.NonRddBlock = NonRddBlock;
