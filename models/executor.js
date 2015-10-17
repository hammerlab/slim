
var maybeParseInt = require("../utils/utils").maybeParseInt;
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;
var NonRddBlock = require("./block").NonRddBlock;

function Executor(app, id) {
  this.appId = app.id;
  this.id = id;

  this.app = app;

  this.init(
        [ 'appId', 'id' ],
        {
          maxMem: { sums: [ app ] }
        }
  );

  this.blocks = {};
  this.upsertHooks.push(this.updateMemUsedPercent);
}

mixinMongoMethods(Executor, "Executor", "Executors");

Executor.prototype.getBlock = function(blockId) {
  if (!(blockId in this.blocks)) {
    this.blocks[blockId] = new NonRddBlock(this, blockId);
  }
  return this.blocks[blockId];
};

Executor.prototype.updateMemUsedPercent = function() {
  if (this.get('MemorySize') && this.get('maxMem')) {
    this.set('MemPercent', this.get('MemorySize') / this.get('maxMem'), true);
  }
  return this;
};

Executor.prototype.remove = function() {
  this.unset('maxMem', true);
};

function getExecutorId(executorId) {
  if (typeof executorId == 'object') {
    if ('Block Manager ID' in executorId) {
      executorId = executorId['Block Manager ID'];
    }
    executorId = executorId['Executor ID'];
  }
  return maybeParseInt(executorId);
}

module.exports.Executor = Executor;
module.exports.getExecutorId = getExecutorId;
