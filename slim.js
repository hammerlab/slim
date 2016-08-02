#!/usr/bin/env node

var argv = require('minimist')(process.argv.slice(2));

var l = require('./utils/log').l;

var mongoPort = argv.p || argv['mongo-port'] || 3001;
var mongoHost = argv.h || argv['mongo-host'] || 'localhost';
var mongoDb = argv.d || argv['mongo-db'] || 'meteor';
var mongoUrl = argv.u || argv['mongo-url'] || ('mongodb://' + mongoHost + ':' + mongoPort + '/' + mongoDb);
var meteorUrl = argv.m || argv['meteor-url'];
if (meteorUrl) {
  var sh = require('shelljs');
  var o = sh.exec('meteor mongo -U ' + meteorUrl);
  if (o.code) {
    l.error("Couldn't get Mongo URL for Meteor server:", meteorUrl);
  } else {
    mongoUrl = o.output.trim();
  }
}

if (!mongoUrl.match(/^mongodb:\/\//)) {
  mongoUrl = 'mongodb://' + mongoUrl;
}

var Server = require('./server').Server;
new Server(mongoUrl);
