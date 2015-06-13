
var colls = require('./collections');
var deq = require('deep-equal');

var l = require('./log').l;

var upsertOpts = { upsert: true, returnOriginal: false };
var upsertCb = function(event) {
  return function(err, val) {
    if (err) {
      l.error("ERROR (%s): %O", event, err);
    } else {
      l.info("Added %s: %O", event, val);
    }
  }
};

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

function addIncProp(clazz) {
  clazz.prototype.inc = function(key, i) {
    if (i === undefined) {
      i = 1;
    }
    if (i == 0) return this;
    return this.set(key, (this.get(key) || 0) + i, true);
  };
}

function addDecProp(clazz) {
  clazz.prototype.dec = function(key, i) {
    if (i === undefined) {
      i = 1;
    }
    if (i == 0) return this;
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

function mixinMongoMethods(clazz, className, collectionName) {
  addSetProp(clazz, className);
  addIncProp(clazz);
  addDecProp(clazz);
  addGetProp(clazz);
  addProcessTime(clazz);
  addUpsert(clazz, className, collectionName);
};

function toSeq(m) {
  var ret = [];
  for (k in m) {
    ret.push([k, m[k]]);
  }
  return ret;
}

function removeKeySpaces(obj) {
  if (typeof obj !== 'object') return obj;
  var ret = {};
  for (k in obj) {
    ret[k.replace(/ /g, '')] = removeKeySpaces(obj[k]);
  }
  return ret;
}

function combineObjKey(ret, a, b, k, combineFn) {
  if (typeof a[k] == 'number') {
    if (b && k in b && typeof b[k] != 'number') {
      l.error("Found {%s:%d} in %O but {%s:%s} in %O", k, a[k], a, k, b[k], b);
    } else {
      ret[k] = combineFn(a[k], b && b[k] || 0);
    }
  } else if (typeof a[k] == 'object') {
    if (b && k in b && typeof b[k] != 'object') {
      l.error("Found {%s:%O} in %O but {%s:%s} in %O", k, a[k], a, k, b[k], b);
    } else {
      ret[k] = combineObjs(a[k], b && b[k] || {}, combineFn);
    }
  } else {
    ret[k] = a[k];
  }
}

function combineObjs(a, b, combineFn) {
  var ret = {};
  if (a) {
    for (k in a) {
      combineObjKey(ret, a, b, k, combineFn);
    }
  }
  if (b) {
    for (k in b) {
      if (a && k in a) continue;
      combineObjKey(ret, b, a, k, combineFn);
    }
  }
  return ret;
}

function sub(a, b) { return a - b; }
function add(a, b) { return a + b; }

function subObjs(a, b) {
  return combineObjs(a, b, sub);
}

function addObjs(a, b) {
  return combineObjs(a, b, add);
}

function maxObjs(a, b) {
  return combineObjs(a, b, Math.max);
}

function flattenObj(o, prefix, ret) {
  if (typeof o != 'object') return o;
  ret = ret || {};
  prefix = prefix || '';
  var prefixDot = prefix ? (prefix + '.') : '';
  for (k in o) {
    ret[prefixDot] = flattenObj(o[k], prefixDot + k, ret);
  }
  return ret;
}

module.exports = {
  PENDING: undefined,
  RUNNING: 1,
  SUCCEEDED: 2,
  FAILED: 3,
  SKIPPED: 4,
  status: {
    PENDING: "PENDING",
    RUNNING: "RUNNING",
    FAILED: "FAILED",
    SUCCEEDED: "SUCCEEDED",
    SKIPPED: "SKIPPED"
  },
  toSeq: toSeq,
  removeKeySpaces: removeKeySpaces,
  upsertOpts: upsertOpts,
  upsertCb: upsertCb,
  mixinMongoMethods: mixinMongoMethods,
  flattenObj: flattenObj,
  addObjs: addObjs,
  subObjs: subObjs,
  maxObjs: maxObjs
};
