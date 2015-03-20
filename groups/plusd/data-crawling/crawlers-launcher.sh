#!/usr/bin/env node

var sys = require('sys');
var childProcess = require('child_process');
var sleep1Cmd = 'sleep 1';
var sleep3Cmd = 'sleep 3';
var args = process.argv.slice(2);

function execute(cmd) {
	return childProcess.execSync(cmd, {
		encoding: 'utf8',
		shell: '/bin/bash'
	});
}
function executeCwd(cmd, cwdPath) {
	return childProcess.execSync(cmd, {
		cwd: cwdPath,
		encoding: 'utf8',
		shell: '/bin/bash'
	});
}

var errors = {
	'NONE': 0,
	'BAD_NB_ARGUMENTS': 1,
	'JAR_DOES_NOT_EXISTS': 2
};

// Load arguments
if(args.length != 4) {
	var scriptName = './'+__filename.split('/').pop();
	var elements = [scriptName, '<nb of docs to crawl for each crawler>',
		'<nb of crawlers to launch>', '<doc no to begin from>',
		'<jar file location>'];
	console.error('Cmd use: '+elements.join(' '));
	process.exit(errors['BAD_NB_ARGUMENTS']);
}

var stepSize, nbCrawlers, from, jarLocation;
args.forEach(function(val, index) {
	switch(index) {
		case 0:
			stepSize = parseInt(val);
			break;
			
		case 1:
			nbCrawlers = parseInt(val);
			break;
			
		case 2:
			from = parseInt(val);
			break;
		
		default:
			jarLocation = val;
	}
			
});

cmd = 'test -e '+jarLocation+'; echo $?';
var exists = (parseInt(execute(cmd)) === 0);
if(!exists) {
	console.error('Jar file('+jarLocation+') does not exist!');
	process.exit(errors['JAR_DOES_NOT_EXISTS']);
}

// Launch crawlers
var cFrom = from;
var cTo = from+stepSize-1;
for(var crawlerNo = 0; crawlerNo < nbCrawlers; crawlerNo++) {
	console.log('Launching crawler '+(crawlerNo+1)+'/'+nbCrawlers);
	
	// Create dir
	var dirName = cFrom+'-'+cTo;
	cmd = 'test -e '+jarLocation+'; echo $?';
	var dirExists = (parseInt(execute(cmd)) === 0);
	
	if(!dirExists) {
		cmd = 'mkdir '+dirName;
		execute(cmd);
	}
		
	// Copy jar into
	cmd = 'cp '+jarLocation+' '+dirName;
	execute(cmd);
	
	// Launch the jar
	cmd = 'java -jar cables-crawler-java-only.jar '+cFrom+' '+cTo+' &> log.txt &';
	executeCwd(cmd, dirName);
	
	var logExists;
	do {
		console.log('.. waiting for log.txt to be created');
		execute(sleep1Cmd);
		cmd = 'test -e log.txt; echo $?';
		logExists = (parseInt(executeCwd(cmd, dirName)) === 0);
	} while(!logExists);
	
	cmd = '(grep -Fq "fetched" log.txt || grep -Fq "Exiting" log.txt); echo $?';
	var fetchedADoc;
	do {
		console.log('.. waiting for 1st pages to be fetched');
		execute(sleep3Cmd);
		fetchedADoc = (parseInt(executeCwd(cmd, dirName)) === 0);
	} while(!fetchedADoc);
	
	// aknowledge
	console.log('.. crawler launched =)) !');
	
	// Iterate
	cFrom = cTo+1;
	cTo = cFrom+stepSize-1;
}
