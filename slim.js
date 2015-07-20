
var argv = require('minimist')(process.argv.slice(2));

var mongoPort = argv.p || argv['mongo-port'] || 3001;
var mongoHost = argv.h || argv['mongo-host'] || 'localhost';
var mongoDb = argv.d || argv['mongo-db'] || 'meteor';
var mongoUrl = argv.m || argv['mongo-url'] || ('mongodb://' + mongoHost + ':' + mongoPort + '/' + mongoDb);

if (!mongoUrl.match(/^mongodb:\/\//)) {
  mongoUrl = 'mongodb://' + mongoUrl;
}

var Server = require('./server').Server;
new Server(mongoUrl);

