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

var input = process.argv[2];
var basename = input.split('.').slice(0, -1).join('.');

fs.readFile(input, function (err, data) {  
  var file = data.toString();
  console.log("Read " + input + ' length =', file.length, 'bytes');
  var db = user.bench(file);
  
  var file2 = user.write_db(db);
  fs.writeFileSync(basename + '.json', file2);
  console.log("Saved " + basename + '.json, length =', file2.length, 'bytes');
  
  var db2 = user.read_db(file2);

  var file3 = user.write_db(db2);
  fs.writeFileSync(basename + '_roundtrip.json', file3);
  console.log("Saved " + basename + '_roundtrip.json, length =', file3.length, 'bytes');
});
