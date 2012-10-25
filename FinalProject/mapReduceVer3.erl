%% Author: 
%% Created: Aug 16, 2012
%% Description: TODO: Add description to mapReduce2
-module(mapReduceVer3).
-export([start/2,sizeOfTable/1,areTheyNeighbours/6]).

-define(NumOfCores,erlang:system_info(logical_processors_available)).
-define(SPREAD,100).
-define(COMPUTERS,1).

%% -define (C1, 	'S1@132.72.246.240').
%% -define (C2, 	'S2@132.72.245.204').
%% -define (C3, 	'S3@10.0.0.2').
%% -define (C4, 	'S4@10.0.0.2').
%% -define (MAIN, 	'Main@127.0.0.1').	% This is Where fold and write run (UI)
%% -define (ListOfStations, [?MAIN,?C1,?C2,?C3,?C4]).

%% A function that calcs the number of Vertices in the ETS
sizeOfTable(ETS)->
	Details=ets:info(ETS),
	{_value,Size}=lists:nth(6,Details),
	Size.	

%% A function that returns [] if the key is unfound or has no colleagus, or returns a list if key has colleagues
checkIfNeighbour(BigETS,NewGuy,PeopleSoFar,WritePID,CounterPID)->
	Value = ets:lookup(BigETS, NewGuy),
	case (Value) of
		[] 					->	NewGuyFriendsList=[];					%NewGuy is not in ETS
		[{NewGuy,[]}] 		->	NewGuyFriendsList=[];					%NewGuy is in ETS but has no colleges 
		[{NewGuy,Colleagues}] ->	NewGuyFriendsList= Colleagues		%NewGuy is in ETS and has colleges 	
	end,
	ThreadPID=self(),													%self PID
	NewGuyFriendsLength=length(NewGuyFriendsList),
	NeighboursLength=length(PeopleSoFar),
	case (NewGuyFriendsLength) of
		0	->	Temp=[];												%new guy has no neighbours
		_	->	case (NeighboursLength>NewGuyFriendsLength) of			%if there are neighbours, check which is shorter and test it on the other
				true  ->	Temp=[spawn(fun() -> areTheyNeighbours(Candidate,PeopleSoFar,WritePID,NewGuy,CounterPID,ThreadPID) end) || Candidate <- NewGuyFriendsList];	%NeighboursLength is longer
				false ->	Temp=[spawn(fun() -> areTheyNeighbours(Candidate,NewGuyFriendsList,WritePID,NewGuy,CounterPID,ThreadPID) end) || Candidate <- PeopleSoFar]	%NewGuyFriendsLength is longer
				end
	end,
	TempLength=length(Temp),
	waitForThreads(TempLength,CounterPID).


%% A function that waits for threads to finish working
waitForThreads(0,CounterPID)			->	CounterPID!{processDone};
waitForThreads(TempLength,CounterPID)	->	
	receive
		{threadDone}->ok
	end,
	waitForThreads(TempLength-1,CounterPID).	

	
%% A function that checks if a given name is in a list, if not' add him
areTheyNeighbours(Candidate,List,WritePID,NewGuy,CounterPID,ThreadPID)->
	Shave =  fun(X) -> 	if X == Candidate -> true; 			%define shave fun
						  			true -> false end 
			 			end,
	IsThere=lists:any(Shave,List),							%check if Candidate is in List ()
	case (IsThere) of
		true ->		CounterPID!{add},						%notify counter that another thing need to be added to file
					Answer = {friends, NewGuy, Candidate};	%add new friends
		false->		Answer = {nofriends}					%no new friends
	end,
	WritePID!Answer,										%send answer
	ThreadPID!{threadDone}.									%send thread done to proccess




start(BigETS, NeededMore ) ->	
	RankETS = ets:new(bigETS, [ordered_set,public ]),
	{ok,Name3}=dets:open_file(degreeDets,[{access,read}]),
	RankETS = dets:to_ets(Name3,RankETS),
	ok=dets:close(Name3),
	Key=ets:last(RankETS),									%get most connected rank in RankETS
	[{FirstRank,PeopleLeft}] = ets:lookup(RankETS,Key),		%get a list with all the people with the highest rank
	ets:delete(RankETS,FirstRank),							%delete highest rank
	{ok,File_Name} = first_Time_Write_To_File(),
	MainPID=self(),
	CounterPID=spawn(fun()-> counter(0,MainPID,0) end),		%start counter
	WritePID=spawn(fun()-> writingThread(File_Name,MainPID,CounterPID) end),
	NewGuy=lists:last(PeopleLeft),							%get a person from the highest rank list
	WritePID!{add,NewGuy,FirstRank},						%add new Guy's label
	UpdatedNewPeople=lists:delete(NewGuy,PeopleLeft),		%delete the person we took and update the list
	PeopleSoFar=[NewGuy],									%update visited persons
	buildGraph(NeededMore-1, RankETS, BigETS, PeopleSoFar, UpdatedNewPeople,WritePID, FirstRank,CounterPID),
	waitForFinish(RankETS).

%% A function that adds top N people to graph
buildGraph(0, _RankETS, _BigETS, _PeopleSoFar, _PeopleLeft,WritePID, _CurrentRank,CounterPID)-> 
																	CounterPID!{mainDone},				%ask status from counter
																	waitForDotFile(WritePID,CounterPID);%wait for counter to reach 0
%get next Rank level
buildGraph(NeededMore, RankETS, BigETS, PeopleSoFar, []		   ,WritePID, _CurrentRank,CounterPID)	->
	Key=ets:last(RankETS),									%get next most connected rank in RankETS
	[{FirstRank,PeopleLeft}] = ets:lookup(RankETS,Key),		%get a list with all the people with the highest rank
	ets:delete(RankETS,FirstRank),							%delete highest rank
	buildGraph(NeededMore, RankETS, BigETS, PeopleSoFar, PeopleLeft,WritePID, FirstRank,CounterPID);

%get more people in that rank level		
buildGraph(NeededMore, RankETS, BigETS, PeopleSoFar, PeopleLeft,WritePID, CurrentRank,CounterPID)	->
	NewGuy=lists:last(PeopleLeft),
	WritePID!{add,NewGuy,CurrentRank},
	UpdatedNewPeople=lists:delete(NewGuy,PeopleLeft),
	CounterPID!{newProcess},
	spawn(fun()->checkIfNeighbour(BigETS,NewGuy,PeopleSoFar,WritePID,CounterPID)end),
	case (lists:member(NewGuy,PeopleSoFar)) of
		true -> NewPeopleSoFar=PeopleSoFar;
		false->	NewPeopleSoFar=[NewGuy]++PeopleSoFar
	end,
	
	buildGraph(NeededMore-1, RankETS, BigETS, NewPeopleSoFar, UpdatedNewPeople,WritePID, CurrentRank, CounterPID).
	

doneWithAll(WritePID)				->	WritePID!{finish}.
%% A function where main waits for counter to reach 0,0 , ask every periot of time 
waitForDotFile(WritePID,CounterPID)	->		
	CounterPID!{areYouDone},				%ask if counter is 0,0
	receive
		{no} 	-> 	timer:sleep(5),			%if not done, wait before re-asking
				   	waitForDotFile(WritePID,CounterPID);
		{yes} 	-> 	doneWithAll(WritePID)	%all processess and writing to file are done
	end.		

%% this counter counts open threads and attemps to write to file- which takes time, to avoid quiting prog before file has written
counter(NumOfLinesToWrite,MainPID,Counter)->
%	io:format("{NumOfLinesToWrite,Counter} is ~p~n",[{NumOfLinesToWrite,Counter}]),
	receive
		{processDone}->	NewCounter=Counter-1,					%a process has been don
						NewNum=NumOfLinesToWrite;
		{newProcess}->  NewCounter=Counter+1,					%new process spawned
						NewNum=NumOfLinesToWrite;	
		{areYouDone}->	NewCounter=Counter,						%a question by main, when counter is 0 the prog can end
						NewNum=NumOfLinesToWrite,
						case ({NumOfLinesToWrite,Counter}) of
						{0,0}	->	MainPID!{yes};				%counter is 0,0
						_		-> 	MainPID!{no}				%still processess or attempt to write to file
						end;	
		{remove}	->	NewCounter=Counter,						%decrease counter end of writing to file
						NewNum=NumOfLinesToWrite-1;
		{add}		->	NewCounter=Counter,						%increse counter- new writing to file
						NewNum=NumOfLinesToWrite+1
	end,
	counter(NewNum,MainPID,NewCounter).	
		



%% A function for adding name and neighbours to file
writingThread(File_Name,MainPID,CounterPID) ->
	receive 
		{add,NewGuy,Rank}			->	addLabel(File_Name, NewGuy,Rank),
										writingThread(File_Name,MainPID,CounterPID);
		{friends, NewGuy, Candidate}-> 	addConnection(File_Name, NewGuy, Candidate,CounterPID),
										writingThread(File_Name,MainPID,CounterPID);
		{nofriends}					->  writingThread(File_Name,MainPID,CounterPID);
		{finish}					->	finish_Write_To_File(MainPID, File_Name)
	end.

%% A function for the first time the dot file opens, adds head of file
first_Time_Write_To_File() -> 
	io:format("~n Creating DOT File... ~n"), 
	{ok,Name} = file:open("Graph3.dot",[write]),
	file:write(Name, "graph ranked_graph{\n"),
	file:write(Name, "graph [fontsize=1000];\n"),
	file:write(Name, "edge  [fontsize=1000];\n"),
	file:write(Name, "node  [fontsize=1000];\n"),
	file:write(Name, "ranksep = 200;\n"),
	file:write(Name, "nodesep = 50;\n"),
	file:write(Name, "edge [style=\"setlinewidth(13)\"]\n"),
	file:write(Name, "node [style=\"setlinewidth(43)\"]\n"),	
	io:format("~n Done creating DOT File... ~n"), 
	{ok,Name}.

%% A function for adding names to the dot file
addLabel(File_Name, NewGuy,Rank) -> 
	RankString=integer_to_list(Rank),
	Write_Head = "\""++ NewGuy ++"\"" ++ "[label= \"" ++ NewGuy ++ " " ++ RankString ++ "\"]\n",
%	Write_Head = "\""++ NewGuy ++"\"" ++ "[label= \"" ++ NewGuy ++ "\"]\n",

	file:write(File_Name,Write_Head),
	ok.

%% A function for adding names to the dot file
addConnection(File_Name, Person1, Person2,CounterPID) -> 
	Write_Head =  "\""++ Person1 ++"\"" ++ "--" "\""++ Person2 ++"\"" ++ "\n",
	file:write(File_Name,Write_Head),
	CounterPID!{remove},
	ok.

%% A function for the last time the dot file opens, adds end of file
finish_Write_To_File(MainPID, File_Name)-> 
	file:write(File_Name,"}"),file:close(File_Name),
	io:format("~n...Finish writing to DOT file ~n"),
	io:format("Job done, uploading Image file.~n"),
	spawn(os,cmd,["xdot Graph3.dot"]),
	MainPID!finish,
	ok. 


waitForFinish(RankETS)->
receive
finish -> io:format("~n...Finished  mapReduce 3...~n"),
		  ets:delete(RankETS),
		  ok
end.

