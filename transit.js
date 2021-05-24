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
  console.log('transit.edn length =', file.length, 'bytes');
  var db = user.bench(file);
  
  var file2 = user.write_db(db);
  fs.writeFileSync('transit.json', file2);
  console.log('transit.json length =', file2.length, 'bytes');
  
  var db2 = user.read_db(file2);

  var file3 = user.write_db(db2);
  fs.writeFileSync('transit_2.json', file3);
  console.log('transit_2.json length =', file3.length, 'bytes');
});
