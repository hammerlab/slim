
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;
var NonRddBlock = require("./block").NonRddBlock;

function Executor(appId, id) {
  this.appId = appId;
  this.id = id;
  this.init([ 'appId', 'id' ]);

  this.blocks = {};
}

mixinMongoMethods(Executor, "Executor", "Executors");

Executor.prototype.getBlock = function(blockId) {
  if (!(blockId in this.blocks)) {
    this.blocks[blockId] = new NonRddBlock(this, blockId);
  }
  return this.blocks[blockId];
};


module.exports.Executor = Executor;
