%% Authors: alon Rolnik, Ron Schmid
%% Created: Aug 16, 2012
%% Description: TODO: Add description to mapReduce2
-module(mapReduce2).
-define(NumOfCores,erlang:system_info(logical_processors_available)).
-define(SPREAD,100).
-define(COMPUTERS,1).
%-define(ETS,tempDets).
-define(ETS,myDets).
%a flag for choosing number of cores for each computer
-define(CoresToUse,1).			%"0" => use Maximum cores available
								%"1" =>	use limitation mentioned in COMP#CORE for eaqch computer


-define (C1, 	'S1@132.72.245.204').		-define(COMP1CORE,4).
-define (C2, 	'S2@132.72.245.204').		-define(COMP2CORE,4).
-define (C3, 	'S3@132.72.245.204').		-define(COMP3CORE,4).
-define (C4, 	'S4@132.72.245.204').		-define(COMP4CORE,4).
-define (ListOfStations, [?C1,?C2,?C3,?C4]).

-export([start/1, openTable/1, initializeWorker/2, work/2]).
%%the program starts here, here we define number of computers, core in each computer and initialize tables and vars
start(BigETS) -> 
		MainPID=self(),		
		CompList=lists:sublist(?ListOfStations, ?COMPUTERS),								%use number of computers defined
		[spawn(Where,mapReduce2,initializeWorker,[MainPID,Where]) || Where <- CompList],	%spawn workers in other COMPUTERS
		ThreadList=initializeMain(?COMPUTERS,[]),											%open threads as # of overall cores
		{ok,File_Name} = first_Time_Write_To_File(),										%write header of txt file
		TablesPids = [spawn(C,mapReduce2,openTable,[MainPID]) || C <- CompList],			%create ETS Tables on every computer
		getAcks(?COMPUTERS),																%wait for tables to open before moving on
		Workers = [spawn(C,mapReduce2,work,[bigETS,sortETS]) || C <- ThreadList],	%open threads on remote computers (# of cores times)
		start_Spread(BigETS, ets:first(BigETS), File_Name, Workers,TablesPids).				%spread keys to remote computer			

%% A function for obtaining PID of the workers
initializeMain(0,ThredList)		->	ThredList;						%retuurn ThreadList
initializeMain(I,ThredList)		->
		receive
			{stationDone}		->	initializeMain(I-1,ThredList);	%move to next station
			{stationID,Name}	->	NewThredList=[Name]++ThredList,	%update threadList
									initializeMain(I,NewThredList)	%go back to loop
		end.	

%% A function for setting number of cores for each computer.
%% The number of cores can be as detected by erlang or by user limitation
initializeWorker(MainPID,Name)->	
		case (?CoresToUse) of													%uses flag "CoresToUse" in define which method to use
			0->NumberOfCores=erlang:system_info(logical_processors_available);	%use MAXIMUM number of core for this certain computer
			1->	case (Name) of													%use limit number of core for this certain computer
					'S1@132.72.245.204'	->	NumberOfCores=?COMP1CORE;
					'S2@132.72.245.204'	->	NumberOfCores=?COMP2CORE;
					'S3@132.72.245.204'	->	NumberOfCores=?COMP3CORE;
					'S4@132.72.245.204'	->	NumberOfCores=?COMP4CORE;
					_					->	NumberOfCores=erlang:system_info(logical_processors_available)	
				end																%default = maximum available cores
		end,
		sendNumOfCores(MainPID,Name,NumberOfCores).								%sent NumberOfCores times the answer to main


%% A function that sends NumberOfCores times the ID of the computer to main
sendNumOfCores(MainPID,_Name, 0)			->	MainPID!{stationDone};		%done
sendNumOfCores(MainPID,Name, NumberOfCores)	->	MainPID!{stationID,Name},	%send computer ID
												sendNumOfCores(MainPID,Name, NumberOfCores-1).

%%a function that makes start wait here until all acks from remote computers had arrived, after opening ETS tables 
getAcks(0) -> 	ok;
getAcks(I) -> 	receive						%received new ack
					ack -> getAcks(I-1)		%go back to loop
				end.

%% This function spreads blocks of keys from BigETS to remote stations for them to calculater ranked sorted ETS tables
start_Spread(BigETS,First_Key, File_Name, Workers,TablesPids)->
				
		case (First_Key) of														%gets first key of block
			'$end_of_table' ->													%if done
				io:format("~n end of table... signal to processes... send stop...~n"),
				MyPID=self(),
				[P!{stop,MyPID} || P <- Workers],								%sends stop to all workers
				io:format("~n Wait for Workers to finish and empty their queues...~n"),
				wait(File_Name,BigETS,length(Workers),Workers,TablesPids, 0);	%waits for threads to finish their queues
			_->
				Temp_Key = spread(First_Key, BigETS, Workers),					%send block of keys to workers
				start_Spread(BigETS,Temp_Key, File_Name, Workers,TablesPids)	%go back for a new block
		end.

%% a function that waits for threads to finish their queues
wait(File_Name,BigETS,NumOfWorkers,Workers,TablesPids, NumOfWorkers)->
		MyPID=self(),											%mainPID
		ets:delete(BigETS),										%when done, delete ETS table
		[P!{startMerge,self()} || P <- Workers],				%spawn workers for the SortETS tables merge
		NewSortETS = ets:new(newSortETS, [ordered_set,public]),	%open a new sortETS
		case (?CoresToUse) of
			0->NumOfCoresToUse=?NumOfCores;
			1->NumOfCoresToUse=?COMP1CORE
		end,		
		CoreWorkers = [spawn(fun() -> getNames(MyPID,NewSortETS) end) || _ <-lists:seq(1, NumOfCoresToUse)],
		io:format("CoreWorkers are: ~p.~n",[CoreWorkers]),
		merge(Workers,TablesPids,NewSortETS,NumOfWorkers,0,CoreWorkers),	%start the merge
		finish(NewSortETS, File_Name);							%go to finish when merge is complete

wait(File_Name,BigETS,NumOfWorkers,Workers,TablesPids, Counter)->  
										receive
											finish -> wait(File_Name,BigETS,NumOfWorkers,Workers,TablesPids, Counter+1)
										end.

%% A function for merging all small sortETS to a united one
merge(Workers,TablesPids, _SortETS, Length, Length,CoreWorkers) -> 
		[Core!tellMeWhenUDone || Core <-CoreWorkers],	%tell workers we're done
		getAcks(length(CoreWorkers)),					%wait for workers to finish queue
		[P!stop || P <- TablesPids++Workers],ok;		%kill all open threads

merge(Workers,TablesPids, SortETS, Length, Counter,CoreWorkers) ->
	receive
		{Key,Value} -> 	Where=lists:nth(random:uniform(length(CoreWorkers)),CoreWorkers),	%randomize a core to update this key and value
						Where!{Key,Value},													%send to core
						merge(Workers,TablesPids,SortETS, Length, Counter,CoreWorkers);		%back to the loop
		done -> 		merge(Workers,TablesPids,SortETS, Length, Counter+1,CoreWorkers)	%back to the loop, advance counter
	end.	

%% A function for the core workers, when a key and value arrives, it updates th sortETS TABLE
getNames(MyPID,SortETS)->
	receive
		{Key,Value} -> 		
						case (ets:lookup(SortETS, Key)) of							%lookup key
							[]-> ets:insert(SortETS, {Key,Value});					%if no record of an author whit this rank, add new
							[{Key,Val}]-> NewVal = handleDataNew:union(Value, Val),	%if record exists, pipe it to value
							ets:insert(SortETS, {Key,NewVal})						%upate value
						end,
						getNames(MyPID,SortETS);									%back to listening in the loop
	tellMeWhenUDone	->	io:format("CoreWorker ~p is done.~n",[self()]),
						MyPID!ack
	end.

%% A function that picks block of ?SPREAD keys and sends it to a random thread on a remote computer
spread(FirstKey, BigETS, Workers) -> 
		{Key, List_Of_Keys} = give_N_Keys_From_First(BigETS,FirstKey,?SPREAD,0,[FirstKey]),	%gets a block starting from last used position
		lists:nth(random:uniform(length(Workers)),Workers)!{List_Of_Keys},					%send to random thread
		Key.																				%return last used key

%% A function that returns a block of ?SPREAD keys startinf from First variable key
give_N_Keys_From_First(_ETS,First,N,N,List_Of_Returned_Keys) -> {First, List_Of_Returned_Keys};

give_N_Keys_From_First(ETS,First,N,Counter,List_Of_Returned_Keys) -> 
	Next = ets:next(ETS, First),															%fetch next Key
	case Next of																			%check if it's the last key in ETS 
		'$end_of_table' -> {Next, List_Of_Returned_Keys};									%return List_Of_Returned_Keys
		Next -> give_N_Keys_From_First(ETS,Next,N,Counter+1,[Next] ++ List_Of_Returned_Keys)%go again
	end.

%% A function that turns ETS into DETS, closes the file, and starts writing txt file
finish(SortETS, File_Name) -> 
		io:format("~n...Convert ETS degree to DETS degree called degreeDets. ~n"),
		{ok,Name}=dets:open_file(degreeDets,[{type,set}]),
		Name = ets:to_dets(SortETS,Name),
		ok=dets:close(Name),
		loop_Writing(SortETS,ets:last(SortETS), File_Name).

%% A function that writes all authors and their ranks in a txt file 
loop_Writing(SortETS, First, File_Name) ->
		case (First) of
			'$end_of_table' -> 	file:close(File_Name),										%all done
								io:format("Finished writing to degree file ~n"),			%print to screen
							   	ets:delete(SortETS),										%delete sortETS
							   	map2!{stop},												
							   	ok;
			Key 			-> 	[{Key,Val}] = ets:lookup(SortETS, Key),						%new key, val to write
				 				NewVal = "\nDegree is:\s " ++ integer_to_list(Key) ++ "\s\s\sAuthors are:\s ",
				   				[ok=file:write(File_Name,NewVal ++ Author) || Author <- lists:sort(Val)],
								Next = ets:prev(SortETS,Key),
								loop_Writing(SortETS, Next, File_Name)						%back to loop
		end.

%% A function that opens sortETS and BigETS once on every remote computer.
%% The function will not open a second BigETS in the computer that runs the start of the program
%% This function runs for as long as the tables are needed.
openTable(MainPid) -> 
case ets:info(bigETS) of									%checks If bigETS has already been built
		undefined 	->										%if not, create it from the DETS table
						ETS = ets:new(bigETS, [set,public,named_table,{read_concurrency, true}]),
						{ok,Name2}=dets:open_file(?ETS,[{access,read}]),
						ETS = dets:to_ets(Name2,ETS),
						ok=dets:close(Name2);
		_ 			->	ok																%otherwise, do nothing
		end,
		
		ets:new(sortETS, [ordered_set,public,named_table, {write_concurrency,true}]),	%create sortETS
		MainPid!ack,																	%send ack when done	
		receive
			stop -> ok																	%kill process, dump tables
		end.

%% A function for the workers. 
%% Incharge of adding received keys to sortETS, and to send finish msg when done, and sending sortETS for merging
work(BigETS, SortETS)->
		receive
			{List_Name} 	-> 	[check_For_Name(BigETS, SortETS, Name) || Name <- List_Name],  	%add new names to sortETS
								work(BigETS, SortETS);											%back to listen in the loop
			{stop,Pid} 	 	-> 	Pid!finish,				%send finish when queue is empty and done dealling with all Keys
						   		work(BigETS, SortETS);											%back to listen in the loop
			{startMerge,Pid} -> startMerge(SortETS,Pid,ets:first(SortETS))						%start sending sortETS for merging		
		end.

startMerge(SortETS,MainPid,First) -> 
		case (First) of										%checkes if done
			'$end_of_table' -> io:format("~p Finished Sending SortedETS to main ~n",[self()]),
							   MainPid!done,				%sends done to main
							   receive 
								   stop -> ok				%waits for ack for done
							   end;
			Key -> 	
					case ets:lookup(SortETS, Key) of									%gets all values for given key
						[] 			-> startMerge(SortETS,MainPid,ets:first(SortETS));	%no value for given key
						[{Key,Val}] -> 
											Next = ets:next(SortETS,Key),				%get next key
											ets:delete(SortETS, Key),					%delete key from table
										   	MainPid !{Key,Val},							%send key and value to main				
											startMerge(SortETS,MainPid,Next)
					end
		end.

%% A function that updates sortETS
check_For_Name(BigETS,SortETS, Name) ->
		[{Name,Colleagues}] = ets:lookup(BigETS, Name),				%lookup for key
		Length_Of_Colleagues = length(Colleagues),					%calc length of neighbours
		Value = ets:lookup(SortETS, Length_Of_Colleagues),			%find rank
		case (Value) of
			[]			-> 	ets:insert(SortETS, {Length_Of_Colleagues,[Name]});	%if empty, add new key
			[{Key,Val}]	-> 	NewVal = [Name]++Val,								%if found pipe to val
						  	ets:insert(SortETS, {Key,NewVal})					%update value
		end.

%% A function for writting the header of ETS = ets:new(bigETS, [set,public,named_table,{read_concurrency, true}])the txt file
first_Time_Write_To_File() -> 
		io:format("~nOpen the degrees file... Start ,MapReduce 2... ~n"),	%print on terminal
		{ok,Name} = file:open("degrees.txt",[write]),						%create degrees.txt
		file:write(Name, "This is The Authors Degree file sorted from high to low :\n"),
		{ok,Name}.															%return given name of file
