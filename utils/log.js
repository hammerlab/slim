
var argv = require('minimist')(process.argv.slice(2));

function pad(s, n, p) {
  p = p || ' ';
  var pads = '';
  for (var i = 0; i < n - s.toString().length; i++) {
    pads += p;
  }
  return pads + s;
}

module.exports.l = require('tracer').colorConsole({
  level: argv.l || 'info',
  format: [
    "{{timestamp}} {{title}} {{file}}:{{line}}: {{message}}",
    {
      error: "{{timestamp}} {{title}} {{file}}:{{line}}: {{message}}\nCall Stack:\n{{stack}}"
    }
  ],
  dateformat : "yyyy/mm/dd HH:MM:ss",
  preprocess: function(data) {
    data.title = data.title.toUpperCase()[0];
    var spaces = '';
    for (var i = 0; i < 15 - data.file.length; i++) {
      spaces += ' ';
    }
    var lastDot = data.file.lastIndexOf('.');
    if (lastDot > 0) {
      data.file = data.file.substring(0, lastDot);
    }
    data.file = pad(data.file, 13);
    data.line = pad(data.line, 3);
  }
});
