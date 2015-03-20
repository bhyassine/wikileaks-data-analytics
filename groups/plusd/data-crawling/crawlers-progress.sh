#!/usr/bin/env node

var sys = require('sys');
var childProcess = require('child_process');
var cmd;

cmd = 'find . -type d -name "pages-crawled"';
var outputDirsStr = childProcess.execSync(cmd, {
	encoding: 'utf8'
});

var outputDirs = outputDirsStr.split('\n').map(function(str) {
	return str.trim();
}).filter(function(str) {
	return str.length != 0;
});

outputDirs.push('.');

outputDirs.forEach(function(outputDir) {
	cmd = 'find '+outputDir+' -type f -name "*.gz" | wc -l';
	var crawledCount = parseInt(childProcess.execSync(cmd, {
		encoding: 'utf8'
	}), 10);
	
	console.log(outputDir, crawledCount);
});


cmd = 'du -sh $(pwd)';
var size = childProcess.execSync(cmd, {
	encoding: 'utf8'
});
console.log('Size of current folder', size);
