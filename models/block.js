
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;


function RddBlock(rdd, id) {
  this.appId = rdd.appId;
  this.rddId = rdd.id;
  this.id = id;

  this.findObj = { appId: this.appId, rddId: this.rddId, id: this.id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;
  this.applyRateLimit = true;
}

RddBlock.prototype.isCached = function() {
  return this.get("MemorySize") || this.get("DiskSize") || this.get("ExternalBlockStoreSize");
};

mixinMongoMethods(RddBlock, "RddBlock", "RddBlocks");


function NonRddBlock(executor, id) {
  this.appId = executor.appId;
  this.execId = executor.id;
  this.id = id;

  this.findObj = { appId: this.appId, execId: this.execId, id: this.id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;
  this.applyRateLimit = true;
}

mixinMongoMethods(NonRddBlock, "NonRddBlock", "NonRddBlocks");


module.exports.RddBlock = RddBlock;
module.exports.NonRddBlock = NonRddBlock;
