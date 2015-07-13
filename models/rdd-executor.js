
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function RDDExecutor(rdd, executorId) {
  this.appId = rdd.appId;
  this.rddId = rdd.id;
  this.execId = executorId;

  this.init([ 'appId', 'rddId', 'execId' ]);

}

mixinMongoMethods(RDDExecutor, 'RDDExecutor', 'RDDExecutors');

module.exports.RDDExecutor = RDDExecutor;
