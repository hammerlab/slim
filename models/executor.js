
var mixinMongoMethods = require("../mongo/record").mixinMongoMethods;

function Executor(appId, id) {
  this.appId = appId;
  this.id = id;
  this.findObj = { appId: appId, id: id };
  this.propsObj = {};
  this.toSyncObj = {};
  this.dirty = true;
  this.key = [ "app", appId, "executor", id ].join('-');
  this.applyRateLimit = true;
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
  var rdds = [];
  if (!updatedBlocks) return rdds;
  updatedBlocks.forEach(function(block) {
    var blockId = block['BlockID'];
    var blockIdKey = 'blocks.' + blockId;

    var rddIdMatch = blockId.match(/^rdd_([0-9]+)_([0-9]+)$/);
    var rdd = null;
    var rddKey = null;
    if (rddIdMatch) {
      var rddId = parseInt(rddIdMatch[1]);
      var rddIndex = parseInt(rddIdMatch[2]);
      rddKey = ['blocks', 'rdd', rddId].join('.');
      blockIdKey = [rddKey, 'blocks', rddIndex].join('.');
      rdd = app.getRDD(rddId);
      rdds.push(rdd);
    }

    var status = block['Status'];
    ['MemorySize', 'DiskSize', 'ExternalBlockStoreSize'].forEach(function(key) {
      var delta = status[key] - (this.get(blockIdKey + '.' + key) || 0);
      this.inc(key, delta).inc(rddKey + '.' + key, delta);
      app.inc(key, delta);
      if (rdd) rdd.inc(key, delta);
    }.bind(this));
    if (isNone(status["StorageLevel"])) {
      if (blockIdKey in this.propsObj) {
        this.unset(blockIdKey);
        this.dec('numBlocks').dec(rddKey + '.numBlocks');
        if (rdd) rdd.dec("numCachedPartitions")
      }
    } else {
      if (!(blockIdKey in this.propsObj)) {
        this.inc('numBlocks').inc(rddKey + '.numBlocks');
        if (rdd) rdd.inc("numCachedPartitions")
      }
      this.set(blockIdKey, status);
    }
  }.bind(this));
  return rdds;
};

module.exports.Executor = Executor;
