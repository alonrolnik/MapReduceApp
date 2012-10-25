%% Authors: alon Rolnik, Ron Schmid
%% Created: Aug 1, 2012
%% Description: TODO: Add description to mapReduce
-module(mapReduceMAIN).
-export([start/0,initialize/0]).
%-define (ETS,tempDets).
-define (ETS,myDets).

initialize()-> xmlToDets:start().			%a prog for building a DETS table from a XML file

start() -> 
		register(mapRed,self()),			%register this PID as mapRed
		WXPid = spawn (wxBuilderF,start,[]),%start graphic interface
		%builds an ETS table from a DETS file
		ETS = ets:new(bigETS, [set,public,named_table, {read_concurrency,true} ]),
		{ok,Name}=dets:open_file(myDets,[{access,read}]),
		ETS = dets:to_ets(Name,ETS),
		ok=dets:close(Name),
		wait(WXPid,ETS).					%wait in loop for UI


wait(WXPid, ETS)->
	receive
%Sehif 1		
		{WXPid,conTree,AuthorName,Depth} -> io:format("Request for author: ~p in depth ~p received!! ~n",[AuthorName,Depth]),
											%spawn(mapReduce1,start,[ETS, AuthorName, Depth]),	%calc result
											T=timer:tc(mapReduce1,start,[ETS, AuthorName, Depth]),%calc result
											io:format("The time this took: ~p~n",[T]),			%print time
											WXPid!done,											%send ack to UI
											wait(WXPid,ETS);									%back to loop
%Sehif 2		
		{WXPid,rankTable} 				-> 	io:format("Request for converting ETS table to Rank Table !! ~n"),
											%spawn(mapReduce2,start,[ETS]),						%calc result
											T=timer:tc(mapReduce2,start,[ETS]),					%calc result
											io:format("The time this took: ~p~n",[T]),			%print time
											WXPid!done,											%send ack to UI
											wait(WXPid,ETS);	
%Sehif 3		
		{WXPid,getSize}			->			SizeOfTable=mapReduceVer3:sizeOfTable(ETS),			%calc ETS size
											WXPid!{sizeIs,SizeOfTable},							%sent ETS size to UI
											wait(WXPid,ETS);									%back to loop
		{WXPid,eliteGroup,N}	-> 			io:format("Request for top ~p authers received!! ~n",[N]),
											%spawn(mapReduceVer3,start,[ETS, N]),				%calc result
											T=timer:tc(mapReduceVer3,start,[ETS, N]),			%calc result
											io:format("The time this took: ~p~n",[T]),			%print time
											WXPid!done,											%send ack to UI
											wait(WXPid,ETS);									%back to loop
%General
		{stop} -> unregister(mapRed), 		%unregister name
				  ets:delete(ETS), 			%delete ETS table
				  io:format("Thank you, All done and finished~n"),		
				  ok						%GOODBYE!!!!
	end.