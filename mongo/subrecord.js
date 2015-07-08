
var record = require('./record');

function addSuperSetProp(clazz, className) {
  clazz.prototype.set = function(key, val, allowExtant) {
    if (typeof key == 'string') {
      this.super.set(this.superKey + key, val, allowExtant);
    } else if (typeof key == 'object') {
      for (k in key) {
        this.set(k, key[k], !!val);
      }
    } else {
      throw new Error("Invalid " + className + ".super.set() argument: " + key);
    }
    return this;
  };
}

function addSuperGetProp(clazz) {
  clazz.prototype.get = function(key) {
    return this.super.get(this.superKey + key);
  }
}

function addSuperUnsetProp(clazz) {
  clazz.prototype.unset = function(key) {
    return this.super.unset(this.superKey + key);
  }
}

function addSuperIncProp(clazz) {
  clazz.prototype.inc = function(key, i) {
    return this.super.inc(this.superKey + key, i);
  }
}

function addSuperDecProp(clazz) {
  clazz.prototype.dec = function(key, i) {
    return this.super.dec(this.superKey + key, i);
  }
}

function mixinMongoSubrecordMethods(clazz, className) {
  addSuperSetProp(clazz, className);
  addSuperUnsetProp(clazz);
  addSuperGetProp(clazz);
  addSuperIncProp(clazz);
  addSuperDecProp(clazz);
  record.addSetDuration(clazz);
  clazz.prototype.upsert = function() {};
}

module.exports = {
  mixinMongoSubrecordMethods: mixinMongoSubrecordMethods
};
