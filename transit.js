#!/usr/local/bin/node

var fs = require('fs'),
    vm = require('vm');

global.performance = {now: function () {
  var t = process.hrtime();
  return Math.round(t[0] * 1000 + t[1] / 1000000);
}}

global.goog = {};

function nodeGlobalRequire(file) {
  vm.runInThisContext.call(global, fs.readFileSync(file), file);
}

nodeGlobalRequire('./target/datascript.js');

fs.readFile('transit.edn', function (err, data) {
  var file = data.toString();
  console.log(file.length);
  user.bench(file);
});
