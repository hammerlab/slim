
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;


function RddBlock(rddExecutor, id) {
  this.appId = rddExecutor.appId;
  this.rddId = rddExecutor.rddId;
  this.id = id;

  this.execId = rddExecutor.execId;

  var callbackObj = {};
  ['MemorySize', 'DiskSize', 'ExternalBlockStoreSize'].forEach((key) => {
    callbackObj[key] = {
      sums: [
        rddExecutor, rddExecutor.rdd, rddExecutor.executor, rddExecutor.executor.app
      ]
    };
  });

  this.init(
        ['appId', 'rddId', 'id'],
        callbackObj
  );
  this.set('execId', this.execId);
}

RddBlock.prototype.isCached = function() {
  return this.get("MemorySize") || this.get("DiskSize") || this.get("ExternalBlockStoreSize");
};

RddBlock.prototype.remove = function() {
  [ 'MemorySize', 'DiskSize', 'ExternalBlockStoreSize' ].forEach((key) => {
    this.unset(key, true);
  });
};

mixinMongoMethods(RddBlock, "RddBlock", "RddBlocks");


function NonRddBlock(executor, id) {
  this.appId = executor.appId;
  this.execId = executor.id;
  this.id = id;

  var callbackObj = {};
  ['MemorySize', 'DiskSize', 'ExternalBlockStoreSize'].forEach((key) => {
    var renamedSumsObj = {};
    renamedSumsObj["nonRdd." + key] = [ executor, executor.app ];
    callbackObj[key] = {
      renamedSums: renamedSumsObj
    };
  });

  this.init(
        [ 'appId', 'execId', 'id' ],
        callbackObj
  );
}

mixinMongoMethods(NonRddBlock, "NonRddBlock", "NonRddBlocks");


module.exports.RddBlock = RddBlock;
module.exports.NonRddBlock = NonRddBlock;
