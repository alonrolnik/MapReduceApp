%% Authors: alon Rolnik, Ron Schmid
%% Created: Aug 1, 2012
%% Description: TODO: Add description to mapReduce
-module(mapReduce).
-export([start1/0,start2/0,start3/0,initialize/0]).
-define(NumOfCores,erlang:system_info(logical_processors_available)).
%-define(ETS,tempDets).
-define(ETS,myDets).


initialize()-> xmlToDets:start().
start1() -> spawn(fun() -> map1() end).
start2() -> map2().
start3() -> map3().

map1()->
		io:format("Num of cores: ~p",[?NumOfCores]),
		register(map1,self()),
		WXPid = spawn (wxBuilder,start,[]),
		ETS = ets:new(bigETS, [set,public, {read_concurrency,true} ]),
		{ok,Name}=dets:open_file(?ETS,[{access,read}]),
		ETS = dets:to_ets(Name,ETS),
		ok=dets:close(Name),
		wait1(WXPid,ETS).

map2() ->
		register(map2,self()),
		ETS = ets:new(bigETS, [set,public,named_table,{read_concurrency, true}]),
		{ok,Name2}=dets:open_file(?ETS,[{access,read}]),
		ETS = dets:to_ets(Name2,ETS),
		ok=dets:close(Name2),
		spawn(mapReduce2,start,[ETS]),
	receive
		{stop} -> io:format("Part 2 finished~n"),unregister(map2),ok
	end.

map3() ->
		register(map3,self()),
		WXPid = spawn (wxBuilder3,start,[]),
		
		BigETS = ets:new(bigETS, [set,public, {read_concurrency,true} ]),
		{ok,Name4}=dets:open_file(myDets,[{access,read}]),
		BigETS = dets:to_ets(Name4,BigETS),
		ok=dets:close(Name4),		
		wait3(WXPid,BigETS).

wait1(WXPid, ETS)->
	receive
		{WXPid,conTree,AuthorName,Depth} -> WXPid!done,
											spawn(mapReduce1,start,[ETS, AuthorName, Depth]),
											wait1(WXPid,ETS);
		{stop} -> unregister(map1), ets:delete(ETS), io:format("Part 1 finished~n"),ok
	end.

wait3(WXPid,BigETS)->
	receive
		{WXPid,getSize}			->			SizeOfTable=mapReduceVer3:sizeOfTable(BigETS),
											WXPid!{sizeIs,SizeOfTable},
											wait3(WXPid,BigETS);
		{WXPid,eliteGroup,N}	-> 			io:format("Request for top ~p authers received!! ~n",[N]),
											WXPid!done,
											spawn(mapReduceVer3,start,[BigETS, N]),
											wait3(WXPid,BigETS);
		{stop} -> unregister(map3), ets:delete(BigETS), io:format("Part 3 finished~n"),ok
	end.

