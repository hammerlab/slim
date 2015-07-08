
var argv = require('minimist')(process.argv.slice(2));

var l = require('../utils/log').l;
var m = require('moment');

var colls = require('./collections');
var deq = require('deep-equal');

var upsertOpts = { upsert: true, returnOriginal: false };
var upsertCb = function(event) {
  return function(err, val) {
    if (err) {
      l.error("ERROR (%s): %O", event, err);
    } else {
      l.debug("Added %s: %O", event, val);
    }
  }
};

var UpsertStats = require('./upsert-stats').UpsertStats;
var upsertStats = new UpsertStats();

var useRealInc = !argv.f && !argv['fake-inc'];

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
        this.set(k, key[k], !!val);
      }
    } else {
      throw new Error("Invalid " + className + ".set() argument: " + key);
    }
    return this;
  }
}

function addUnset(clazz) {
  clazz.prototype.unset = function(key) {
    if (key in this.propsObj) {
      this.unsetKeys = this.unsetKeys || [];
      this.unsetKeys.push(key);
      delete this.propsObj[key];
      delete this.toSyncObj[key];
      this.dirty = true;
    }
    return this;
  }
}

function addIncProp(clazz) {
  clazz.prototype.inc = function(key, i) {
    if (i === undefined) {
      i = 1;
    }
    if (i == 0) return this;
    if (useRealInc) {
      if (!this.incObj) this.incObj = {};
      this.incObj[key] = (this.incObj[key] || 0) + i;
      this.propsObj[key] = (this.propsObj[key] || 0) + i;
      this.dirty = true;
      return this;
    } else {
      return this.set(key, (this.get(key) || 0) + i, true);
    }
  };
}

function addDecProp(clazz) {
  clazz.prototype.dec = function(key, i) {
    if (i === undefined) {
      i = 1;
    }
    if (i == 0) return this;
    return this.inc(key, -i);
  };
}

function addGetProp(clazz) {
  clazz.prototype.get = function(key) {
    return this.propsObj[key];
  }
}

function isEmptyObject(o) {
  for (k in o) return false;
  return true;
}

function addUpsert(clazz, className, collectionName) {
  clazz.prototype.upsert = function() {
    if (!this.dirty) return this;
    if (this.applyRateLimit) {
      if (this.blocking) {
        this.blocked = true;
        return this;
      } else {
        this.blocking = true;
      }
    }

    var upsertObj = { };
    if (!isEmptyObject(this.toSyncObj)) {
      upsertObj['$set'] = this.toSyncObj;
    }
    if (this.unsetKeys) {
      upsertObj['$unset'] = {};
      this.unsetKeys.forEach(function(unsetKey) {
        upsertObj['$unset'][unsetKey] = 1;
      });
      this.unsetKeys = null;
    }
    if (this.incObj && !isEmptyObject(this.incObj)) {
      upsertObj['$inc'] = this.incObj;
    }
    if (!upsertObj['$inc']) {
      upsertObj['$inc'] = {};
    }
    upsertObj['$inc']['n'] = 1;

    upsertStats.inc();
    var b = {
      t: m(),
      started: upsertStats.started,
      ended: upsertStats.ended
    };
    colls[collectionName].findOneAndUpdate(
          this.findObj,
          upsertObj,
          upsertOpts,
          function(err, val) {
            var after = m();
            upsertStats.dec();
            this.blocking = false;
            if (err) {
              l.error("ERROR (%s): %O", className, err);
            } else {
              l.debug("Added %s: %O", className, val);
              if (className == 'Stage') {
                var v = val.value;
                l.info(
                      'After upsert (started %d->%d, ended %d->%d, in flight %d->%d): Stage %d: %d running, %d succeeded, took %d ms',
                      b.started, upsertStats.started,
                      b.ended, upsertStats.ended,
                      b.started - b.ended, upsertStats.started - upsertStats.ended,
                      v.id,
                      v.taskCounts && v.taskCounts.running || 0,
                      v.taskCounts && v.taskCounts.succeeded || 0,
                      after - b.t
                );
              }
              if (this.blocked) {
                this.blocked = false;
                this.upsert();
              }
            }
          }.bind(this)
    );
    this.toSyncObj = {};
    this.incObj = {};
    this.dirty = false;
    return this;
  };
}

function mixinMongoMethods(clazz, className, collectionName) {
  addSetProp(clazz, className);
  addUnset(clazz);
  addIncProp(clazz);
  addDecProp(clazz);
  addGetProp(clazz);
  addUpsert(clazz, className, collectionName);
}

module.exports = {
  upsertOpts: upsertOpts,
  upsertCb: upsertCb,
  mixinMongoMethods: mixinMongoMethods
};
