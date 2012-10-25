%% Author: Alon Rolnik, Ron Schmid
%% Created: Aug 16, 2012
%% Description: runup file for uploading the program.
-module(a).
-export([a/0]).

a()-> 	
io:format("~n~n~n~n Compiling files~n~n~n~n"),
	compile:file(handleDataNew),
	compile:file(mapReduce),
	compile:file(mapReduce1),
	compile:file(mapReduce2),
	compile:file(mapReduceVer2),
	compile:file(mapReduce3),
	compile:file(mapReduceVer3),	

	compile:file(wxBuilderFinal),
	compile:file(wxBuilderF),
	compile:file(wxBuilder),
	compile:file(wxBuilder3),
	compile:file(initilizer),

	compile:file(xmlToDets),
	io:format("Run Prog~n~n~n~n"),
	mapReduce:start4().
