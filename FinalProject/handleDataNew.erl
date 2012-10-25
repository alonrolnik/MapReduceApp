%% Author Ron Schmid 021759220
%% This is a test


-module(handleDataNew).
-export ([addNames/2, union/2]).
	
addNames(A,List)->
	LislLen=length(List),
	go(A,List,LislLen,0).


go(_A,_List,LislLen,LislLen)->	ok;
go(A,List,LislLen,I)->		
				First=lists:nth(1,List),
				ShortList=lists:sublist(List, 2, LislLen-1),	
				Value=ets:lookup(A, First),
				case (Value) of
				[]-> NewVal=Value;
				_->[{_Key,NewVal}]=Value
				end,
				NewCompanions=union(ShortList, NewVal),
				ets:insert(A,{First,NewCompanions}),
				LastInList=[First],
				NewList=ShortList ++ LastInList,
				go(A,NewList,LislLen,I+1).
				



delKelem(List , Elem) -> 
	del(List , Elem , []).

del([Elem | T ] , Elem , Temp) ->
    	del( T , Elem , Temp  );
	del([] , _Elem , Temp) -> reverse(Temp);
del([H|T] , Elem , Temp) ->
    	del(T , Elem , [H|Temp]).


union(List1 , List2) ->
    	clean(List1 ++ List2 , []).
	clean([] , Temp) -> reverse(Temp);
	clean([H|T] , Temp) -> clean( delKelem(T , H) ,[ H  |  Temp ] ).

reverse( List ) ->
    	temp(List , []).
temp([] , L) -> L;
temp([H|T] , L) -> 
    temp(T , [ H|L ]).

