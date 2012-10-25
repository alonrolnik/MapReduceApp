%% Author: alon
%% Created: Aug 14, 2012
%% Description: TODO: Add description to xmlToDets
-module(xmlToDets).

-export([start/0]).
-define(chunk, 1024).
-define(NumOfCores,erlang:system_info(logical_processors_available)).

start()->
	io:format("~n Start initialize ... ~n"),
	BigETS = ets:new(bigEts, [set,public]),
	Workers = [spawn(fun() -> work(BigETS) end) || _ <- lists:seq(1,?NumOfCores)],

	HandlPid = spawn(fun() -> handl(BigETS,Workers) end),
	register(handle,HandlPid),
	
  F = fun get_authors/2,   %% the callback function that handles the sax events
  G = fun continue_file/2, %% the callback function that returns the next 
                           %% chunk of data
  %% open file
  io:format("...Open XML File. ~n"),
  {ok, Handle} = file:open(xml(), [read, raw, binary]),
  Position=0,
  CState = {Handle, Position, ?chunk},
  SaxCallbackState = undefined,
  %% erlsom:parse_sax() returns {ok, FinalState, TrailingBytes},
  %% where TrailingBytes is the rest of the input-document
  %% that follows after the last closing tag of the XML, and Result 
  %% is the value of the State after processing the last SAX event.
   
	{ok, _Result, _TrailingBytes} =  erlsom:parse_sax(<<>>, SaxCallbackState, F, 
    [{continuation_function, G, CState}]),file:close(Handle),	
	convertEtsToDets(BigETS,Workers).

convertEtsToDets(BigETS,Workers)->
	%Check to see if all other workers done with their work when their queues are empty
	Sttus = [{message_queue_len,_Length}=process_info(Pid, message_queue_len) || Pid <- Workers],
	io:format("~n queue status: ~p ~n",[Sttus]),
	Chck  = [S || {_,S}<-Sttus , S=:=0],NumOfCores = ?NumOfCores,
	case (length(Chck)) of
		
		NumOfCores -> handle!stop, [P!stop || P <- Workers],continue(BigETS);
		_ -> convertEtsToDets(BigETS,Workers)
	end,
	ok.

continue(BigETS)->
	io:format("...Create DETS file called MyDets. ~n"),
	{ok,Name}=dets:open_file(myDets,[{type,set}]),
	io:format("...Convert ETS to DETS. ~n"),
	Name = ets:to_dets(BigETS,Name),
	ok=dets:close(Name),
	ets:delete(BigETS),
	io:format("~n finish converting,,,~nInitialize done. ~n").
	
%% this is a continuation function that reads chunks of data 
%% from a file.
continue_file(Tail, {Handle, Offset, Chunk}) ->
  %% read the next chunk
  case file:pread(Handle, Offset, Chunk) of
    {ok, Data} ->
      {<<Tail/binary, Data/binary>>, {Handle, Offset + Chunk, Chunk}};
    eof ->
      {Tail, {Handle, Offset, Chunk}}
  end.

%% The input is the sax-event and the state. 
%% The output is the new state.
%% 
%% The state consists of a stack that corresponds to 
%% the level in the XML, the result: [{Author, Colleagues}].
%% Additionally there is a field 'element_acc' which contains
%% an intermediate result while parsing character data,
%% because there can be more than 1 character event per element (in theory).
-record(state, {stack = [none,none], author = [] , colleague = []}).
get_authors(startDocument, _) ->
	#state{stack = [none,none], author=[], colleague=[]};

%get_authors({startElement, _, "article", _, _}, #state{stack = [none,none]} = State) ->
  	%State#state{stack = [start,none],author=[], colleague=[]};

get_authors({startElement, _, "author", _, _}, #state{stack = [none,none]} = State) ->
  	State#state{stack = [author,none],author=[], colleague=[]};

get_authors({characters, Value}, 
            #state{stack = [author,none]} = State)->
  	State#state{author=Value, colleague=[]};

get_authors({endElement, _, "author", _}, #state{stack = [author,none]} = State) ->
  	State#state{stack = [author,author]};


get_authors({startElement, _, "author", _, _}, #state{stack = [author,author]} = State) ->
  	State#state{stack = [colleague,none]};

get_authors({characters, Value}, 
            #state{stack = [colleague,none] , colleague=Coll} = State)->
  	State#state{colleague = [Value] ++ Coll};

get_authors({endElement, _, "author", _}, #state{stack = [colleague,none]} = State) ->
  	State#state{stack = [colleague,colleague]};

get_authors({startElement, _, "author", _, _}, #state{stack = [colleague,colleague]} = State) ->
  	State#state{stack = [colleague,none]};

get_authors({endElement, _, "title", _}, #state{stack = [colleague,colleague], author = Author , colleague = Colleague}) ->
	handle!{[Author|Colleague]},
	#state{stack=[none,none],author=[],colleague=[]};

%get_authors({endElement, _, "title", _}, #state{stack = [author,author] , author = Author ,colleague = Colleague}) ->
%	handle!{[Author|Colleague]},
%	#state{stack=[none,none],author=[],colleague=[]};

get_authors({endElement, _, "title", _}, #state{stack = [author,author] , author = Author}) ->
	handle!{[Author]},
	#state{stack=[none,none],author=[],colleague=[]};


%get_authors({startElement, _, "title", _, _}, #state{author=Author,colleague=Colleague}) ->

get_authors(endDocument, #state{}=State)->
  State#state{};

get_authors(_, S) -> S.

%% this is just to make it easier to test this little example
xml() -> filename:join([codeDir(), "dblp1.xml"]).
codeDir() ->filename:dirname(code:which(?MODULE)).

work(BigETS) ->
	receive 
		stop -> io:format("Process id stop: ~p~n_",[self()]);
		{[]} -> work(BigETS);
		{List} -> _OK = handleDataNew:addNames(BigETS, List) ,%io:format("Status: ~p PID: ~p    List: ~p ~n_",[OK,self(),List]),
		work(BigETS)
%	after 100 -> work(BigETS)
	end.

handl(BigEts,Workers)->
	receive
		
		{[]} -> handl(BigEts,Workers);
		{List} ->	WorkerPid=lists:nth(random:uniform(?NumOfCores),Workers),
					WorkerPid!{List},
					handl(BigEts,Workers);
		stop -> io:format("Process id stop: ~p~n_",[self()]),ok
	%	after 100 -> handl(BigEts,Workers)
	end.
