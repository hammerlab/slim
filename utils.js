
var colls = require('./collections');
var deq = require('deep-equal');

var l = require('./log').l;

module.exports.PENDING = undefined;
module.exports.RUNNING = 1;
module.exports.SUCCEEDED = 2;
module.exports.FAILED = 3;
module.exports.SKIPPED = 4;

module.exports.status = {
  PENDING: "PENDING",
  RUNNING: "RUNNING",
  FAILED: "FAILED",
  SUCCEEDED: "SUCCEEDED",
  SKIPPED: "SKIPPED"
};

var upsertOpts = { upsert: true, returnOriginal: false };
var upsertCb = function(event) {
  return function(err, val) {
    if (err) {
      l.error("ERROR (" + event + "): ", err);
    } else {
      l.info("Added " + event + ": ", val);
    }
  }
};

module.exports.upsertOpts = upsertOpts;
module.exports.upsertCb = upsertCb;

function addSetProp(clazz, className) {
  clazz.prototype.set = function(key, val, allowExtant) {
    if (typeof key == 'string') {
      if (val === undefined) return this;
      if (key in this.propsObj) {
        if (!deq(this.propsObj[key], val)) {
          if (!allowExtant) {
            throw new Error(
                  "Attempting to set " + key + " to " + val + " on " + className + " with existing val " + this.propsObj[key]
            );
          }
          this.propsObj[key] = val;
          this.toSyncObj[key] = val;
          this.dirty = true;
        }
      } else {
        this.propsObj[key] = val;
        this.toSyncObj[key] = val;
        this.dirty = true;
      }
    } else if (typeof key == 'object') {
      for (k in key) {
        this.set(k, key[k]);
      }
    } else {
      throw new Error("Invalid " + className + ".set() argument: " + key);
    }
    return this;
  }
}

function addIncProp(clazz) {
  clazz.prototype.inc = function(key, i) {
    if (i === undefined) {
      i = 1;
    }
    return this.set(key, (this.get(key) || 0) + i, true);
  };
}

function addDecProp(clazz) {
  clazz.prototype.dec = function(key, i) {
    if (i === undefined) {
      i = 1;
    }
    return this.set(key, (this.get(key) || 0) - i, true);
  };
}

function addGetProp(clazz) {
  clazz.prototype.get = function(key) {
    return this.propsObj[key];
  }
}

function addUpsert(clazz, className, collectionName) {
  clazz.prototype.upsert = function() {
    if (!this.dirty) return this;
    colls[collectionName].findOneAndUpdate(
          this.findObj,
          { $set: this.toSyncObj },
          upsertOpts,
          upsertCb(className)
    );
    this.toSyncObj = {};
    this.dirty = false;
    return this;
  };
}

function addProcessTime(clazz) {
  clazz.prototype.processTime = function(t) {
    return t ? t : undefined;
  };
}

module.exports.mixinMongoMethods = function(clazz, className, collectionName) {
  addSetProp(clazz, className);
  addIncProp(clazz);
  addDecProp(clazz);
  addGetProp(clazz);
  addProcessTime(clazz);
  addUpsert(clazz, className, collectionName);
};

