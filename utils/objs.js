
var l = require('./log').l;

function toSeq(m) {
  var ret = [];
  if (!m) return ret;
  for (k in m) {
    ret.push([k, m[k]]);
  }
  return ret;
}

function removeKeySpaces(obj) {
  return transformKeys(obj, / /g, '');
}

function removeKeyDots(obj) {
  return transformKeys(obj, /\./g, '-');
}

function transformKeys(obj, find, replace) {
  if (obj instanceof Array) {
    return obj.map(function(o) { return transformKeys(o, find, replace); });
  }
  if (typeof obj === 'object') {
    var ret = {};
    for (k in obj) {
      ret[k.replace(find, replace)] = transformKeys(obj[k], find, replace);
    }
    return ret;
  }
  return obj;
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
  ret = ret || {};
  if (typeof o != 'object') {
    ret[prefix] = o;
    return ret;
  }
  prefix = prefix || '';
  var prefixDot = prefix ? (prefix + '.') : '';
  for (k in o) {
    flattenObj(o[k], prefixDot + k, ret);
  }
  return ret;
}

module.exports = {
  toSeq: toSeq,
  removeKeySpaces: removeKeySpaces,
  transformKeys: transformKeys,
  removeKeyDots: removeKeyDots,
  flattenObj: flattenObj,
  addObjs: addObjs,
  subObjs: subObjs,
  maxObjs: maxObjs
};

