
function processTime(t) {
  return t ? t : undefined;
}

module.exports = {
  PENDING: undefined,
  RUNNING: 1,
  SUCCEEDED: 2,
  FAILED: 3,
  SKIPPED: 4
};

module.exports.status = {};
module.exports.status[module.exports.PENDING] = "PENDING";
module.exports.status[module.exports.RUNNING] = "RUNNING";
module.exports.status[module.exports.FAILED] = "FAILED";
module.exports.status[module.exports.SUCCEEDED] = "SUCCEEDED";
module.exports.status[module.exports.SKIPPED] = "SKIPPED";

module.exports.processTime = processTime;
