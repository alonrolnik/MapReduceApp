%% Authors: alon Rolnik, Ron Schmid
%% Created: Aug 16, 2012
%% Description: TODO: Add description to mapReduce1
-module(mapReduce1).

-export([start/3,check_For_Name/4]).
%-compile(export_all).
-define(NumOfCores,erlang:system_info(logical_processors_available)).
-define(COMPUTERS,1).


-define (C1, 	'S1@132.72.245.204').
-define (C2, 	'S2@132.72.245.204').
-define (C3, 	'S3@132.72.245.204').
-define (C4, 	'S4@132.72.245.204').
-define (ListOfStations, [?C1,?C2,?C3,?C4]).

start(BigETS, Root , Depth) ->	
	case (Depth < 0) of 														%validate inserted Value
		true -> io:format("~n Depth can be only non negetive number ~n");		%negative depth
		false ->
			case(ets:member(BigETS, Root)) of									%check if root is in the ETS								
				false -> io:format("~n This name dont exist in the DataBase ~n");%if not
				true ->															%if in ETS
					{ok,File_Name} = first_Time_Write_To_File(Root),			%open and start dot file 
					WritePID=spawn(fun()-> writeToFile(File_Name) end),			%spawn writer prog
					TempETS = ets:new(tempETS, [set, public]),					%create new ETS for knowing if a key was used before
					ets:insert(TempETS,{Root,Root}),							%add root to temp ETS
					start_MapReduce(self(),BigETS, TempETS, WritePID, Root, Depth),%start map reduce1
					waitForFinish(TempETS)										%wait for finish
				end
	end.
%%%%    Initilizer Functions     %%%%
%% A function that initializes the map and fold loops
start_MapReduce(MainPID, BigETS, TempETS, WritePID, Root, Depth)->
	Fold_Pid = spawn(fun() -> fold(MainPID,BigETS, TempETS, Depth, 1, WritePID, 0, 0, 0,[]) end),	% Start the Fold Process Sequential
	spawn(fun()-> check_For_Name(BigETS,Root, Fold_Pid,0) end).										%start map
	

%%a function that checks if a given name is in BigETS
check_For_Name(BigETS, Name, Fold_Pid,CurretDepth) ->
	Value = ets:lookup(BigETS, Name),
	case (Value) of
		[] 					->	Answer = {done, []  , [], 		 CurretDepth};	%Name not in ETS
		[{Name,[]}] 		->	Answer = {done, Name, [], 		 CurretDepth};	%Name in ETS but no colleges 
		[{Name,Colleagues}] ->	Answer = {done, Name, Colleagues,CurretDepth}	%Name in ETS and has colleges 						
	end,
	Fold_Pid!Answer.															%send answer

%% The function that opens maps and collecets their answer every done level
fold(MainPID,_BigETS, _TempETS, 0, 1, WritePID, 0, 0, 0,[])-> 
		WritePID!{finish,MainPID};				%private case- depth= 0

%% Current level is done, going one more depth in to next level if neccesary
fold(MainPID, BigETS, TempETS, MaxDepth, SpawnedMaps, WritePID, SpawnedMaps, CurrentDepth, Newlyspawned, _ListOfSons) 	->	%current depth is done
	case MaxDepth==CurrentDepth+1 of
	 		true  -> WritePID!{finish,MainPID};					%done
			false -> fold(MainPID,BigETS, TempETS, MaxDepth, Newlyspawned, WritePID, 0, CurrentDepth+1, 0, [])%go to next level
	end;

fold(MainPID, BigETS, TempETS, MaxDepth, SpawnedMaps, WritePID, FinishedMaps, CurrentDepth, Newlyspawned, _ListOfSons) 	->	%still in Current depth
	FoldPid=self(),
	receive
 		{done, Name, List_Of_sons, CurrentDepth} -> 
			UpdatedList=testCollInList(Name,TempETS,List_Of_sons),			% shold test if name is in the TempETS if not it should add him, if he has "friends" another map should be spawned for him 
			WritePID!{addToFile, Name, UpdatedList},						%add UpdatedList to dot file
			Where=lists:nth(random:uniform(?COMPUTERS),?ListOfStations),	%randomize to who to send
			case (MaxDepth-CurrentDepth) of									%if its not the last level, spawn more maps so they will wait for us and not the other way around
				1-> ok;														%done, no more levels
				_-> [spawn(Where,mapReduce1,check_For_Name,[BigETS, NewName, FoldPid,CurrentDepth+1]) || NewName <- UpdatedList]%spawn more maps
			end,
			LengthOfUpdatedList=length(UpdatedList)+Newlyspawned,			%get open maps
			fold(MainPID, BigETS, TempETS, MaxDepth, SpawnedMaps, WritePID, FinishedMaps+1, CurrentDepth , LengthOfUpdatedList,UpdatedList)
 	end.



%%check and filter the given list of ListOfSons already apeared before (are present in TempETS)
testCollInList(Name,TempETS,ListOfSons)->
		Colleagues = [Coll || Coll <- ListOfSons , false == ets:member(TempETS, Coll)],		%filter ListOfSons and leave it with only names that arent in TempETS
		[ets:insert(TempETS, {Coll, Name}) || Coll <- Colleagues],							%add Collegues to TempETS
		Colleagues. 																		%return Colleagues
  

%%%% 		dot file functions 		%%%%
   
%% A function for adding name and neighbours to file
writeToFile(File_Name) ->
	receive 
		{addToFile, Head, List_Of_Sons}	->	
				write_To_File(File_Name, Head, List_Of_Sons),
				writeToFile(File_Name);
		{finish,MainPID}				->
				finish_Write_To_File(MainPID, File_Name)
	end.

%% A function for the first time the dot file opens, adds head of file
first_Time_Write_To_File(Head) -> 
	io:format("~n Creating DOT File... ~n"), 
	{ok,Name} = file:open("Graph.dot",[write]),
	file:write(Name, "digraph simple_hierarchy {\n"),
	file:write(Name, "graph [fontsize=1000];\n"),
	file:write(Name, "edge  [fontsize=1000];\n"),
	file:write(Name, "node  [fontsize=1000];\n"),
	file:write(Name, "ranksep = 600;\n"),
	file:write(Name, "nodesep = 80;\n"),
	file:write(Name, "edge [style=\"setlinewidth(13)\"]\n"),
	file:write(Name, "node [style=\"setlinewidth(43)\"]\n"),		
	file:write(Name, "\""++ Head ++"\"" ++ "[label= \"" ++ Head ++ "\"]\n"),  
	io:format("~n Done creating DOT File... ~n"), 
	{ok,Name}.


%% A function for adding names to the dot file
write_To_File(File_Name, Head, List_Of_Sons) -> 
	Write_Head = "\""++ Head ++"\"" ++ "[label= \"" ++ Head ++ "\"]\n",
	file:write(File_Name,Write_Head),
	[file:write(File_Name,"\""++ Coll ++"\"" ++ "[label= \"" ++ Coll ++ "\"]\n") || Coll <- List_Of_Sons],
	[file:write(File_Name, "\""++ Head ++"\"" ++ "->" "\""++ Son ++"\"" ++ "\n") || Son <- List_Of_Sons],
	ok.

%% A function for the last time the dot file opens, adds end of file
finish_Write_To_File(MainPID, File_Name)-> 
	file:write(File_Name,"}"),file:close(File_Name),
	io:format("~n...Finish writing to DOT file ~n"),
	io:format("Job done, uploading Image file.~n"),
	spawn(os,cmd,["xdot Graph.dot"]),
	MainPID!finish,
	ok. 

%%the function waits here for file to finish writting  
waitForFinish(TempETS)->
	receive
	finish -> io:format("~n...Finish mapReduce...~n"),
			  ets:delete(TempETS),
			  ok
	end.
