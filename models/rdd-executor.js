
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function RDDExecutor(rdd, executor) {
  this.appId = rdd.appId;
  this.rddId = rdd.id;
  this.execId = executor.id;

  var callbackObj = {};
  ['MemorySize', 'DiskSize', 'ExternalBlockStoreSize', 'numBlocks'].forEach((key) => {
    callbackObj[key] = { sums: [ rdd.app, rdd, executor ] };
  });
  this.init(
        [ 'appId', 'rddId', 'execId' ],
        callbackObj
  );
}

mixinMongoMethods(RDDExecutor, 'RDDExecutor', 'RDDExecutors');

module.exports.RDDExecutor = RDDExecutor;
