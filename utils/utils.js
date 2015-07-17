
function processTime(t) {
  return t ? t : undefined;
}

function maybeParseInt(n) {
  if (typeof n === 'string' && n.match(/^[0-9]+$/)) {
    return parseInt(n);
  }
  return n;
}

module.exports = {
  PENDING: undefined,
  RUNNING: 1,
  SUCCEEDED: 2,
  FAILED: 3,
  SKIPPED: 4,
  REMOVED: 5
};

module.exports.status = {};
module.exports.status[module.exports.PENDING] = "PENDING";
module.exports.status[module.exports.RUNNING] = "RUNNING";
module.exports.status[module.exports.FAILED] = "FAILED";
module.exports.status[module.exports.SUCCEEDED] = "SUCCEEDED";
module.exports.status[module.exports.SKIPPED] = "SKIPPED";
module.exports.status[module.exports.REMOVED] = "REMOVED";

module.exports.processTime = processTime;
module.exports.maybeParseInt = maybeParseInt;
