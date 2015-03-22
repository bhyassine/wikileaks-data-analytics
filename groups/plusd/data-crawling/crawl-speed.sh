#!/usr/bin/env node

var sys = require('sys');
var childProcess = require('child_process');
var cmd = '';

function count() {
	cmd = 'find . -type f -name "*.gz" | wc -l';
	var countStr = childProcess.execSync(cmd, {
		encoding: 'utf8'
	});
	return parseInt(countStr);
}

function round(x, n) {
	return Math.floor(x*Math.pow(10, n))/Math.pow(10, n);
}

var intervalMs = 5*1000;
var startingTime = new Date().getTime();
var startingCount = count();
var currentCount, currentTime;
var ratioMs, ratioMin;

setInterval(function(){
  currentCount = count();
  currentTime = new Date().getTime();
  
  ratioMs = (currentCount-startingCount)/(currentTime-startingTime);
  ratioMin = ratioMs*1000*60;
  console.log(round(ratioMin, 2), 'docs/minutes');
}, intervalMs);
