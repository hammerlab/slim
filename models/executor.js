
var mixinMongoMethods = require("../utils").mixinMongoMethods;

function Executor(appId, id) {
  this.appId = appId;
  this.id = id;
  this.findObj = { appId: appId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;
}

mixinMongoMethods(Executor, "Executor", "Executors");

function isNone(storageLevel) {
  return !storageLevel["Use Disk"] &&
        !storageLevel["Use Memory"] &&
        !storageLevel["Use ExternalBlockStore"] &&
        !storageLevel["Deserialized"] &&
        storageLevel["Replication"] == 1;
}

Executor.prototype.updateBlocks = function(app, updatedBlocks) {
  if (!updatedBlocks) return this;
  updatedBlocks.forEach(function(block) {
    var blockIdKey = 'blocks.' + block['BlockID'];
    var status = block['Status'];
    ['MemorySize', 'DiskSize', 'ExternalBlockStoreSize'].forEach(function(key) {
      var delta = status[key] - (this.get(blockIdKey + '.' + key) || 0);
      this.inc(key, delta);
      app.inc(key, delta);
    }.bind(this));
    if (isNone(status["StorageLevel"])) {
      if (blockIdKey in this.propsObj) {
        this.unset(blockIdKey);
        this.dec('numBlocks');
      }
    } else {
      if (!(blockIdKey in this.propsObj)) {
        this.inc('numBlocks');
      }
      this.set(blockIdKey, block);
    }
  }.bind(this));
  return this;
};

module.exports.Executor = Executor;
