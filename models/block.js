
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;


function RddBlock(appId, rddId, id) {
  this.appId = appId;
  this.rddId = rddId;
  this.id = id;

  this.findObj = { appId: appId, rddId: rddId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;
  this.applyRateLimit = true;
}

RddBlock.prototype.isCached = function() {
  return this.get("MemorySize") || this.get("DiskSize") || this.get("ExternalBlockStoreSize");
};

mixinMongoMethods(RddBlock, "RddBlock", "RddBlocks");


function NonRddBlock(appId, execId, id) {
  this.appId = appId;
  this.execId = execId;
  this.id = id;

  this.findObj = { appId: appId, execId: execId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;
  this.applyRateLimit = true;
}

mixinMongoMethods(NonRddBlock, "NonRddBlock", "NonRddBlocks");


module.exports.RddBlock = RddBlock;
module.exports.NonRddBlock = NonRddBlock;
