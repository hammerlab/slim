
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function RDDExecutor(rdd, executor) {
  this.appId = rdd.appId;
  this.rddId = rdd.id;
  this.execId = executor.id;

  this.init([ 'appId', 'rddId', 'execId' ]);

}

mixinMongoMethods(RDDExecutor, 'RDDExecutor', 'RDDExecutors');

module.exports.RDDExecutor = RDDExecutor;
