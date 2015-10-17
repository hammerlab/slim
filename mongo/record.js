
var argv = require('minimist')(process.argv.slice(2));
var extend = require('node.extend');

var l = require('../utils/log').l;
var m = require('moment');

var flattenObj = require('../utils/objs').flattenObj;
var isEmptyObject = require('../utils/utils').isEmptyObject;

var colls = require('./collections').collections;
var deq = require('deep-equal');

var PriorityQueue = require('priorityqueuejs');
var UpsertStats = require('./upsert-stats').UpsertStats;

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

var statusLogInterval = argv['status-log-interval'] || 10;

var upsertQueue = new PriorityQueue(function(a,b) {
  if (a.clazz.lowPriority) return -1;
  if (b.clazz.lowPriority) return 1;
  return 0;
});

function getProp(root, key, create, callbackObjs) {
  var segments = key.split('.');
  var parentAndCallbackObj = segments.reduce(
        function(objs, segment, idx) {
          if (!objs[0]) {
            return objs;
          }

          var nextCallbackObj = (objs[1] && (segment in objs[1])) ? objs[1][segment] : null;
          var previousCallbackObj = nextCallbackObj || objs[2];

          if (idx + 1 == segments.length) {
            return [ objs[0], nextCallbackObj, previousCallbackObj ];
          } else if (!(segment in objs[0])) {
            if (create) {
              objs[0][segment] = {};
            } else {
              return [ null, nextCallbackObj, previousCallbackObj ];
            }
          }
          return [ objs[0][segment], nextCallbackObj, previousCallbackObj ];
        },
        [
          root,
          callbackObjs,
          null
        ]
  );
  var parentObj = parentAndCallbackObj[0];

  var callbackObj = parentAndCallbackObj[2];
  if (callbackObjs) {
    if (!callbackObj && (key in callbackObjs)) {
      callbackObj = callbackObjs[key];
    }
  }
  var name = segments[segments.length - 1];
  var exists = parentObj && (name in parentObj);
  return {
    obj: parentObj,
    name: name,
    val: parentObj && parentObj[name],
    handleValueChange(prev, val) {
      if (callbackObj) {
        var sumsConfig = callbackObj.sums;
        if (sumsConfig) {
          sumsConfig.forEach((obj) => {
            obj.inc(key, (val || 0) - (prev || 0));
          });
        }
        var renamedSumsConfig = callbackObj.renamedSums;
        if (renamedSumsConfig) {
          for (var renamedKey in renamedSumsConfig) {
            renamedSumsConfig[renamedKey].map((obj) => {
              obj.inc(renamedKey, (val || 0) - (prev || 0));
            });
          }
        }
        var callbacksConfig = callbackObj.callbacks;
        if (callbacksConfig) {
          callbacksConfig.map((obj) => {
            obj.handleValueChange(prev, val);
          });
        }
      }
    },
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
    return getProp(this.propsObj, key, create, this.propCallbackObj);
  };
  clazz.prototype.enqueue = function() {
    if (!this.enqueued) {
      upsertQueue.enq(this);
      this.enqueued = true;
    }
  };
  clazz.prototype.markChanged = function() {
    if (!this.needsUpsert) {
      this.needsUpsert = true;
      this.enqueue();
    }
  };
  clazz.prototype.set = function(key, val, allowExtant) {
    if (typeof key == 'string') {
      if (val === undefined) return this;
      if (typeof val == 'object' && !(val instanceof Array)) {
        var flattenedVal = flattenObj(val, key);
        for (var subKey in flattenedVal) {
          this.set(subKey, flattenedVal[subKey], allowExtant);
        }
      } else {
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
            var prev = prop.val;
            prop.set(val);
            prop.handleValueChange(prev, val);
            this.toSyncObj[key] = val;
            this.markChanged();
          }
        } else {
          var prev = prop.val;
          prop.set(val);
          prop.handleValueChange(prev, val);
          this.toSyncObj[key] = val;
          this.markChanged();
        }
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
  clazz.prototype.unset = function(key, virtual) {
    var prop = this.getProp(key);
    if (prop.exists) {
      var prev = prop.val;
      if (!virtual) {
        this.unsetKeys = this.unsetKeys || [];
        this.unsetKeys.push(key);
        prop.delete();
        getProp(this.toSyncObj, key).delete();
        delete this.toSyncObj[key];
        this.markChanged();
      }
      prop.handleValueChange(prev, undefined);
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
    if (!this.incObj) this.incObj = {};
    this.incObj[key] = (this.incObj[key] || 0) + i;
    var prop = this.getProp(key, true);
    var prev = prop.val;
    var next = (prop.val || 0) + i;
    prop.set(next);
    prop.handleValueChange(prev, next);
    this.markChanged();
    return this;
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
      prop.obj[prop.name] = [];
    }
    if (!(val in prop.obj[prop.name])) {
      prop.obj[prop.name].push(val);

      this.addToSetObj = this.addToSetObj || {};
      if (!(key in this.addToSetObj)) {
        this.addToSetObj[key] = [];
      }
      this.addToSetObj[key].push(val);
      this.markChanged();
    }
    return this;
  };
}

function addPull(clazz) {
  clazz.prototype.pull = function(key, val) {
    if (!this.pullObj) {
      this.pullObj = {};
    }
    if (key in this.propsObj) {
      var vals = this.propsObj[key];
      var idx = vals.indexOf(val);
      if (idx >= 0) {
        this.propsObj[key].splice(idx, 1);
      }
    }
    var foundUncommittedValue = false;
    if (key in this.toSyncObj) {
      var vals = this.toSyncObj[key];
      var idx = vals.indexOf(val);
      if (idx >= 0) {
        foundUncommittedValue = true;
        this.toSyncObj[key].splice(idx, 1);
      }
    }
    if (key in this.addToSetObj) {
      var vals = this.addToSetObj[key];
      var idx = vals.indexOf(val);
      if (idx >= 0) {
        foundUncommittedValue = true;
        this.addToSetObj[key].splice(idx, 1);
      }
    }
    if (!foundUncommittedValue) {
      this.pullObj[key] = val;
    }
  };
}

function addSetDuration(clazz) {
  clazz.prototype.setDuration = function() {
    var start = this.get('time.start');
    if (start) {
      var end = this.get('time.end');
      var newDur = end ? end - start : Math.max(0, (m().unix() * 1000) - start);
      this.set('duration', newDur, true);
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

var numBlocked = 0;
var maximumInflightUpserts = 1000;

var upsertStats = new UpsertStats(upsertQueue);

setInterval(function() {
  upsertStats.logStatus(numBlocked);
}, statusLogInterval * 1000);


function fireUpserts() {
  while (!upsertQueue.isEmpty() && upsertStats.inFlight < maximumInflightUpserts) {
    var next = upsertQueue.deq();
    next.enqueued = false;
    next.upsert();
  }
}

function addUpsert(clazz, collectionName) {
  clazz.prototype.upsert = function(cb) {
    this.commitHooks.forEach(((hook) => {
      hook.bind(this)();
    }).bind(this));

    if (!this.needsUpsert) {
      return this;
    }

    this.upsertHooks.forEach(function(hook) {
      hook.bind(this)();
    }.bind(this));

    if (this.inFlight) {
      if (!this.blocked) {
        this.blocked = true;
        numBlocked++;
      }
      return this;
    } else if (upsertStats.inFlight >= maximumInflightUpserts) {
      this.enqueue();
      return this;
    } else {
      this.inFlight = true;
    }

    var upsertObj = {};
    if (!isEmptyObject(this.toSyncObj)) {
      upsertObj['$set'] = extend({}, this.toSyncObj);
      this.toSyncObj = {};
    }
    if (this.unsetKeys) {
      upsertObj['$unset'] = {};
      this.unsetKeys.forEach(function(unsetKey) {
        upsertObj['$unset'][unsetKey] = 1;
      });
      this.unsetKeys = null;
    }
    if (this.incObj && !isEmptyObject(this.incObj)) {
      upsertObj['$inc'] = extend({}, this.incObj);
      if (upsertObj['$set']) {
        for (var k in upsertObj['$inc']) {
          if (k in upsertObj['$set']) {
            delete upsertObj['$set'][k];
          }
        }
      }
      this.incObj = {};
    }
    if (!upsertObj['$inc']) {
      upsertObj['$inc'] = {};
    }
    upsertObj['$inc']['n'] = 1;

    if (this.addToSetObj && !isEmptyObject(this.addToSetObj)) {
      var addToSetObj = {};
      for (var k in this.addToSetObj) {
        var vals = this.addToSetObj[k];
        if (vals.length == 1) {
          addToSetObj[k] = vals[0];
        } else if (vals.length > 1) {
          addToSetObj[k] = { $each: vals }
        }
      }
      if (!isEmptyObject(addToSetObj)) {
        upsertObj['$addToSet'] = addToSetObj;
      }
      this.addToSetObj = {};
    }

    if (this.pullObj && !isEmptyObject(this.pullObj)) {
      upsertObj['$pull'] = extend({}, this.pullObj);
      this.pullObj = {};
    }

    if (upsertObj['$addToSet'] && upsertObj['$pull']) {
      var dupedKeys = [];
      for (var k in upsertObj['$addToSet']) {
        if (k in upsertObj['$pull']) {
          dupedKeys.push(k);
          upsertObj['$set'][k] = this.propsObj[k];
          delete upsertObj['$pull'][k];
        }
      }
      dupedKeys.forEach(function(k) {
        delete upsertObj['$addToSet'][k];
      });
      if (isEmptyObject(upsertObj['$addToSet'])) {
        delete upsertObj['$addToSet'];
      }
      if (isEmptyObject(upsertObj['$pull'])) {
        delete upsertObj['$pull'];
      }
    }

    var now = m();
    if (!upsertObj['$set']) {
      upsertObj['$set'] = {};
    }
    upsertObj['$set'].l = now.unix() * 1000;

    upsertStats.inc();
    this.needsUpsert = false;
    colls[collectionName].findOneAndUpdate(
          this.findObj,
          upsertObj,
          upsertOpts,
          function(err, val) {
            upsertStats.dec(now);
            this.inFlight = false;
            if (err) {
              l.error("%s, upserting:", this.toString(), upsertObj, err);
            } else {
              if (this.blocked) {
                this.blocked = false;
                numBlocked--;
                this.upsert();
              } else {
                fireUpserts();
                if (!numBlocked && !upsertStats.inFlight && module.exports.emptyQueueCb) {
                  module.exports.emptyQueueCb();
                }
              }
            }
          }.bind(this)
    );
    return this;
  };
}

function addInit(clazz, className) {
  clazz.prototype.init = function(findKeys, propCallbackObj) {
    this.findObj = {};
    this.propCallbackObj = propCallbackObj;

    this.clazz = className;

    if (!findKeys || !findKeys.length) {
      l.error('%s: no findKeys set', className);
    }

    findKeys.forEach(function(key) {
      if (!(key in this)) {
        l.error("%s: missing findKey %s.", className, key, JSON.stringify(this));
      } else {
        this.findObj[key] = this[key];
      }
    }.bind(this));

    this.propsObj = {};
    this.toSyncObj = {};
    this.markChanged();
    this.upsertHooks = [ this.setDuration ];
    this.commitHooks = [];
  };

  clazz.prototype.toString = function() {
    if (!this.toString_) {
      this.toString_ = this.clazz + "(" + JSON.stringify(this.findObj) + ")";
    }
    return this.toString_;
  }
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
  addSetDuration(clazz);
  addInit(clazz, className);
  addFromMongo(clazz);
  addSetProp(clazz, className);
  addUnset(clazz);
  addIncProp(clazz);
  addDecProp(clazz);
  addGetProp(clazz);
  addHasProp(clazz);
  addPull(clazz);
  addUpsert(clazz, collectionName);
  addAddToSetProp(clazz);
}

module.exports = {
  fireUpserts: fireUpserts,
  upsertOpts: upsertOpts,
  upsertCb: upsertCb,
  mixinMongoMethods: mixinMongoMethods,
  addSetDuration: addSetDuration,
  addAddToSetProp: addAddToSetProp,
  addHasProp: addHasProp
};
