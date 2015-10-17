
var assert = require("assert");
var colls = require("../../mongo/collections");
var fs = require("fs");
var lineReader = require('line-reader');
var recordUtils = require("../../mongo/record");
var handleEvent = require("../../server").handleEvent;
var upsertStats = recordUtils.upsertStats;

function Server(mongoUrl, eventLogFile, cb) {
  colls.init(mongoUrl, function (err) {
    assert.equal(null, err);

    colls.dropDatabase(function(err) {
      var done = false;

      function doneCb() {
        if (done) {
          upsertStats.logStatus(0);
          cb();
        }
      }

      recordUtils.emptyQueueCb = doneCb;
      console.log("replaying event-log file", eventLogFile);
      lineReader.eachLine(eventLogFile, function(line) {
        var e = JSON.parse(line);
        handleEvent(e, doneCb);
      }).then(function() {
        console.log("done!");
        done = true;
      });
    });
  });
}

module.exports.Server = Server;
