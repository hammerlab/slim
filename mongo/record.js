
var argv = require('minimist')(process.argv.slice(2));

var l = require('../utils/log').l;
var m = require('moment');

var flattenObj = require('../utils/objs').flattenObj;

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

function getProp(root, key, create) {
  var segments = key.split('.');
  var parentObj = segments.reduce(function(obj, segment, idx) {
    if (!obj || idx + 1 == segments.length) {
      return obj;
    }
    if (!(segment in obj)) {
      if (create) {
        obj[segment] = {};
      } else {
        return null;
      }
    }
    return obj[segment];
  }, root);
  var name = segments[segments.length - 1];
  var exists = parentObj && (name in parentObj);
  return {
    obj: parentObj,
    name: name,
    val: parentObj && parentObj[name],
    exists: exists,
    set: function(val) {
      if (parentObj) {
        parentObj[name] = val;
      }
    },
    delete: function() {
      if (exists) {
        delete parentObj[name];
      }
    }
  };
}

function addSetProp(clazz, className) {
  clazz.prototype.getProp = function(key, create) {
    return getProp(this.propsObj, key, create);
  };
  clazz.prototype.set = function(key, val, allowExtant) {
    if (typeof key == 'string') {
      if (val === undefined) return this;
      var prop = this.getProp(key, true);
      if (prop.exists) {
        if (!deq(prop.val, val)) {
          if (!allowExtant) {
            l.error(
                  "%s(%s) overwriting %s: %s -> %s",
                  className,
                  JSON.stringify(this.findObj),
                  key,
                  JSON.stringify(prop.val),
                  JSON.stringify(val)
            );
          }
          prop.set(val);
          this.toSyncObj[key] = val;
          this.dirty = true;
        }
      } else {
        prop.set(val);
        this.toSyncObj[key] = val;
        this.dirty = true;
      }
    } else if (typeof key == 'object') {
      for (k in key) {
        this.set(k, key[k], !!val);
      }
    } else {
      l.error(
            "%s(%s): Invalid .set() argument: %s",
            className,
            JSON.stringify(this.findObj),
            JSON.stringify(key)
      );
    }
    return this;
  }
}

function addUnset(clazz) {
  clazz.prototype.unset = function(key) {
    var prop = this.getProp(key);
    if (prop.exists) {
      this.unsetKeys = this.unsetKeys || [];
      this.unsetKeys.push(key);
      prop.delete();
      getProp(this.toSyncObj, key).delete();
      delete this.toSyncObj[key];
      this.dirty = true;
    }
    return this;
  }
}

function addIncProp(clazz) {
  clazz.prototype.inc = function(key, i) {
    if (typeof key === 'object') {
      var flattened = flattenObj(key);
      for (var k in flattened) {
        this.inc(k, flattened[k]);
      }
      return this;
    }
    if (i === undefined) {
      i = 1;
    }
    if (i == 0) return this;
    if (useRealInc) {
      if (!this.incObj) this.incObj = {};
      this.incObj[key] = (this.incObj[key] || 0) + i;
      var prop = this.getProp(key, true);
      prop.set((prop.val || 0) + i);
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

function addAddToSetProp(clazz) {
  clazz.prototype.addToSet = function(key, val) {
    var prop = this.getProp(key, true);
    if (!prop.exists || !prop.val) {
      prop.obj[prop.name] = {};
    }
    if (!(val in prop.obj[prop.name])) {
      prop.obj[prop.name][val] = true;

      this.addToSetObj = this.addToSetObj || {};
      if (!(key in this.addToSetObj)) {
        this.addToSetObj[key] = {};
      }
      this.addToSetObj[key][val] = true;

      this.dirty = true;
    }
    return this;
  };
}

function addSetDuration(clazz) {
  clazz.prototype.setDuration = function() {
    if (!this.get('duration') && this.get('time.start') && this.get('time.end')) {
      this.set('duration', this.get('time.end') - this.get('time.start'));
    }
    return this;
  }
}

function addGetProp(clazz) {
  clazz.prototype.get = function(key) {
    return this.getProp(key).val;
  }
}

function addHasProp(clazz) {
  clazz.prototype.has = function(key) {
    return this.getProp(key).exists;
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

    if (this.addToSetObj && !isEmptyObject(this.addToSetObj)) {
      var addToSetObj = {};
      for (var k in this.addToSetObj) {
        var keyObj = this.addToSetObj[k];
        var elems = [];
        for (var ok in keyObj) {
          elems.push(ok);
        }
        if (elems.length == 1) {
          addToSetObj[k] = elems[0];
        } else if (elems.length > 1) {
          addToSetObj[k] = { $each: elems }
        }
      }
      upsertObj['$addToSet'] = addToSetObj;
    }

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
    this.addToSetObj = {};
    this.incObj = {};
    this.dirty = false;
    return this;
  };
}

function addFromMongo(clazz) {
  clazz.prototype.fromMongo = function(mongoRecord) {
    if (!mongoRecord) return this;
    for (var k in mongoRecord) {
      if (!(k in this.findObj) && k != '_id') {
        this.propsObj[k] = mongoRecord[k];
      }
    }
    return this;
  };
}

function mixinMongoMethods(clazz, className, collectionName) {
  addFromMongo(clazz);
  addSetProp(clazz, className);
  addUnset(clazz);
  addIncProp(clazz);
  addDecProp(clazz);
  addGetProp(clazz);
  addHasProp(clazz);
  addUpsert(clazz, className, collectionName);
  addSetDuration(clazz);
  addAddToSetProp(clazz);
}

module.exports = {
  upsertOpts: upsertOpts,
  upsertCb: upsertCb,
  mixinMongoMethods: mixinMongoMethods,
  addSetDuration: addSetDuration,
  addAddToSetProp: addAddToSetProp,
  addHasProp: addHasProp
};
