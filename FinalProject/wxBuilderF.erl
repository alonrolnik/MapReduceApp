%% Authors: alon Rolnik, Ron Schmid
%% Created: Aug 15, 2012
%% Description: TODO: Add description to wxBuilder
-module(wxBuilderF).
%-compile(export_all).
-export([start/0]).
-include_lib("wx/include/wx.hrl").
 
 
start() ->
    State = make_window(),
    loop (State).
 
make_window() ->
    Server = wx:new(),
    Frame  = wxFrame:new(Server, -1, "MapReduce", [{size,{300, 660}}]),
    Panel  = wxPanel:new(Frame),
 
%% create widgets
%% the order entered here does not control appearance
    T1001 = wxTextCtrl:new(Panel, 1001,[{value, "CUSTOM1"}]), %set default value
    T1002 = wxTextCtrl:new(Panel, 1002,[{value, "CUSTOM2"}]), %set default value
 
	T1003 = wxTextCtrl:new(Panel, 1003,[{value, "Author"}]), %set default value
    T1004 = wxTextCtrl:new(Panel, 1004,[{value, "Depth"}]), %set default value

    ST2001 = wxStaticText:new(Panel, 2001,"Output Area",[]),
    B101  = wxButton:new(Panel, 101, [{label, "&Begin Author's Depth Search"}]),
    B102  = wxButton:new(Panel, ?wxID_EXIT, [{label, "E&xit"}]),
    B103  = wxButton:new(Panel, 103, [{label, "&ROOT N / 10"}]),
    B104  = wxButton:new(Panel, 104, [{label, "&ROOT N / 9"}]),
    B105  = wxButton:new(Panel, 105, [{label, "&ROOT N / 8"}]),
    B106  = wxButton:new(Panel, 106, [{label, "&ROOT N / 7"}]),
    B107  = wxButton:new(Panel, 107, [{label, "&ROOT N / 6"}]),
    B108  = wxButton:new(Panel, 108, [{label, "&ROOT N / 5"}]),
    B109  = wxButton:new(Panel, 109, [{label, "&ROOT N / 4"}]),
    B110  = wxButton:new(Panel, 110, [{label, "&ROOT N / 3 "}]),
    B111  = wxButton:new(Panel, 111, [{label, "&ROOT N / 2"}]),
    B112  = wxButton:new(Panel, 112, [{label, "&ROOT N"}]),
    B113  = wxButton:new(Panel, 113, [{label, "&2 ROOT N"}]),
    B114  = wxButton:new(Panel, 114, [{label, "&3 ROOT N"}]),
    B115  = wxButton:new(Panel, 115, [{label, "&CUSTOM GRAPH 1"}]),
    B116  = wxButton:new(Panel, 116, [{label, "&CUSTOM GRAPH 2"}]),
    B117  = wxButton:new(Panel, 117, [{label, "Get Number Of Vertices "}]),
	B118  = wxButton:new(Panel, 118, [{label, "Turn ETS table To Rank Table"}]),


 %%You can create sizers before or after the widgets that will go into them, but
%%the widgets have to exist before they are added to sizer.
    OuterSizer  = wxBoxSizer:new(?wxHORIZONTAL),
    MainSizer   = wxBoxSizer:new(?wxVERTICAL),
    InputSizer  = wxStaticBoxSizer:new(?wxHORIZONTAL, Panel, [{label, "Good Bye"}]),
	InputSizer2 = wxStaticBoxSizer:new(?wxHORIZONTAL, Panel, [{label, "    Number of vertices to display"}]),
	InputSizer3 = wxStaticBoxSizer:new(?wxHORIZONTAL, Panel, [{label, "Enter an Author And Depth"}]),
	ButtonSizer = wxBoxSizer:new(?wxHORIZONTAL),
	ButtonType1 = wxBoxSizer:new(?wxHORIZONTAL),
	ButtonType2 = wxBoxSizer:new(?wxHORIZONTAL),
	ButtonType3 = wxBoxSizer:new(?wxHORIZONTAL),
	ButtonType4 = wxBoxSizer:new(?wxHORIZONTAL),
	ButtonType5 = wxBoxSizer:new(?wxHORIZONTAL),
	ButtonType6 = wxBoxSizer:new(?wxHORIZONTAL),
 	ButtonType7 = wxBoxSizer:new(?wxHORIZONTAL),

%% Note that the widget is added using the VARIABLE, not the ID.
%% The order they are added here controls appearance.
	wxSizer:addSpacer(MainSizer, 10),  %spacer	
	wxSizer:add(InputSizer3, T1003, []),
    wxSizer:add(InputSizer3, 50, 0, []),
	wxSizer:add(InputSizer3, T1004, []),
 
	wxSizer:addSpacer(MainSizer, 10),  %spacer

	wxSizer:add(MainSizer, InputSizer3,[]),
 
	wxSizer:addSpacer(MainSizer, 5),  %spacer

    wxSizer:add(ButtonSizer, B101,  []),
    wxSizer:add(MainSizer, ButtonSizer, []),
 
	wxSizer:addSpacer(MainSizer, 40),  %spacer

	wxSizer:add(ButtonType7, B118,  []),
    wxSizer:add(MainSizer, ButtonType7, []),
	
	wxSizer:addSpacer(MainSizer, 40),  %spacer

	wxSizer:add(ButtonType6, B117,  []),
    wxSizer:add(MainSizer, ButtonType6, []),
	
%%top group of items 
    wxSizer:addSpacer(MainSizer, 15),  %spacer
 	wxSizer:add(ButtonType1, B103,  []),
    wxSizer:add(ButtonType1, B104,  []),
    wxSizer:add(ButtonType1, B105,  []),
    wxSizer:add(MainSizer, ButtonType1, []),
    wxSizer:addSpacer(MainSizer, 15),  %spacer
%%2nd group of items
    wxSizer:add(ButtonType2, B106,  []),
    wxSizer:add(ButtonType2, B107,  []),
    wxSizer:add(ButtonType2, B108,  []),
    wxSizer:add(MainSizer, ButtonType2, []),
    wxSizer:addSpacer(MainSizer, 15),  %spacer
%%3rd group of items	
	wxSizer:add(ButtonType3, B109,  []),
    wxSizer:add(ButtonType3, B110,  []),
    wxSizer:add(ButtonType3, B111,  []),
    wxSizer:add(MainSizer, ButtonType3, []),
    wxSizer:addSpacer(MainSizer, 15),  %spacer
%%4th group of items	
    wxSizer:add(ButtonType4, B112,  []),
    wxSizer:add(ButtonType4, B113,  []),
    wxSizer:add(ButtonType4, B114,  []),
    wxSizer:add(MainSizer, ButtonType4, []),
	wxSizer:addSpacer(MainSizer, 30),  %spacer
%%input items	
	wxSizer:add(InputSizer2, T1001, []),
    wxSizer:add(InputSizer2, 100, 0, []),
	wxSizer:add(InputSizer2, T1002, []),
    wxSizer:add(MainSizer, InputSizer2,[]),
    wxSizer:addSpacer(MainSizer, 10),  %spacer
%%5th group of items	
    wxSizer:add(ButtonType5, B115,  []),
    wxSizer:add(ButtonType5, B116,  []),
%    wxSizer:add(ButtonType5, B117,  []),
    wxSizer:add(MainSizer, ButtonType5, []),
	wxSizer:addSpacer(MainSizer, 30),  %spacer

%%good bye items	
 % 	wxSizer:add(ButtonSizer, B101,  []),
    wxSizer:add(InputSizer, 50, 0, []),
    wxSizer:add(InputSizer, B102,  []),
    wxSizer:add(MainSizer, InputSizer, []),
    wxSizer:addSpacer(MainSizer, 10),  %spacer	
 
%   wxSizer:add(MainSizer, ST2001, []),
    wxSizer:addSpacer(MainSizer, 10),  %spacer
	
    wxSizer:addSpacer(OuterSizer, 20), % spacer
    wxSizer:add(OuterSizer, MainSizer, []),
 
	wxSizer:add(MainSizer, ST2001, []),
    wxSizer:addSpacer(MainSizer, 10),  %spacer

	
%% Now 'set' OuterSizer into the Panel
    wxPanel:setSizer(Panel, OuterSizer),
    wxFrame:show(Frame),
 
% create two listeners
    wxFrame:connect( Frame, close_window),
    wxPanel:connect(Panel, command_button_clicked),
 
%% the return value, which is stored in State
    {Frame, T1001, T1002, T1003, T1004, ST2001}.
 
loop(State) ->
    {Frame, T1001, T1002, T1003, T1004, ST2001}  = State,  % break State back down into its components
    io:format("--waiting in the loop--~n", []), % optional, feedback to the shell
    receive
 
        % a connection get the close_window signal
        % and sends this message to the server
        #wx{event=#wxClose{}} ->
            io:format("~p Closing window ~n",[self()]), %optional, goes to shell
            %now we use the reference to Frame
         wxWindow:destroy(Frame),  %closes the win1dow
         ok;  % we exit the loop
 
    #wx{id = ?wxID_EXIT, event=#wxCommand{type = command_button_clicked} } ->
            %this message is sent when the exit button (ID 102) is clicked
            io:format("~p Closing window ~n",[self()]), %prints to shell
        	wxWindow:destroy(Frame),					%close window
			mapRed!{stop},								%ends program
         	ok; 											% we exit the loop
 
	 #wx{id = 101, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 101) is clicked
			%show connection tree for author in reseceived depth 
            Author_val1 = wxTextCtrl:getValue(T1003),	%get author	
			Depth_check = wxTextCtrl:getValue(T1004),	%get depth
			case (Depth_check) of						%check if a depth value was inserted
				"Depth" -> Depth_val ="0";				%use 0 as default if nothing was inserted
				Other   -> Depth_val = Other			%else, use inserted value
			end,
			case (Author_val1) of						%check if an author value was inserted
				"Author" -> Author_val ="Yehuda Ben-Shimol";%use Yehuda Ben-Shimol as default if nothing was inserted
				 Else     -> Author_val = Else			%else, use inserted value
			end,
	        wait2(Author_val , Depth_val, ST2001),  	%get results
          	loop(State);								%back to loop
		
    #wx{id = 103, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 103) is clicked
			%get top (ROOT N)/10 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)/10),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
	#wx{id = 104, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 104) is clicked
			%get top (ROOT N)/9 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)/9),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
    #wx{id = 105, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 105) is clicked
			%get top (ROOT N)/8 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)/8),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
	#wx{id = 106, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 106) is clicked
			%get top (ROOT N)/7 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)/7),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
    #wx{id = 107, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 107) is clicked
			%get top (ROOT N)/6 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)/6),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
	#wx{id = 108, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 108) is clicked
			%get top (ROOT N)/5 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)/5),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
   #wx{id = 109, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 109) is clicked
			%get top (ROOT N)/4 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)/4),	%round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
	#wx{id = 110, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 110) is clicked
			%get top (ROOT N)/3 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)/3),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
	#wx{id = 111, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 111) is clicked
			%get top (ROOT N)/2 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)/2),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
	#wx{id = 112, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 112) is clicked
			%get top (ROOT N) authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
   #wx{id = 113, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 113) is clicked
			%get top (ROOT N)*2 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)*2),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
	#wx{id = 114, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 114) is clicked
			%get top (ROOT N)*3 authors
			mapRed!{self(),getSize},				%get total number of authers in table
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			N=trunc(math:sqrt(SizeOfTable)*3),	%calc & round calculation downwards
			wait(N, ST2001),					%get result
            loop(State);						%back to loop
	#wx{id = 115, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 115) is clicked
			% this is a custom number of vertices to display. the default value for this button is 1
			Custom1String = wxTextCtrl:getValue(T1001),		%get inserted value
			case (Custom1String) of
				"CUSTOM1"-> N = 1;							%if no value was inserted, use 1
				 _	     ->	mapRed!{self(),getSize},			%get actual number of vertices
							receive
								{sizeIs,SizeOfTable}->ok		%thanks
							end,
							{Custom1,_Rest}=string:to_integer(Custom1String),	%use inserted value
							case (Custom1<0) of									%value validation
								true 	-> N =0;								%if smaller than 0, use 0
								false 	-> 	case (Custom1>SizeOfTable) of		%check if value is too big
											   true	->	N = SizeOfTable;		%if yes, use max possible value
											   false->	N = Custom1				%if no, use value
											end
							end
			end,
			wait(N, ST2001),				%get results
            loop(State);					%back to loop
	#wx{id = 116, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 116) is clicked
			% this is a custom number of vertices to display. the default value for this button is 1
			Custom2String = wxTextCtrl:getValue(T1002),		%get inserted value
			case (Custom2String) of
				"CUSTOM2"-> N = 2;							%if no value was inserted, use 2
				 _	     ->	mapRed!{self(),getSize},			%get actual number of vertices
							receive
								{sizeIs,SizeOfTable}->ok		%thanks
							end,
							{Custom2,_Rest}=string:to_integer(Custom2String),	%use inserted value
							case (Custom2<0) of									%value validation
								true 	-> N =0;								%if smaller than 0, use 0
								false 	-> 	case (Custom2>SizeOfTable) of		%check if value is too big
											   true	->	N = SizeOfTable;		%if yes, use max possible value
											   false->	N = Custom2				%if no, use value
											end
							end
			end,
			wait(N, ST2001),				%get results
            loop(State);					%back to loop
	#wx{id = 117, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 117) is clicked
			%%this button is used for displaying the total number of vertices
			mapRed!{self(),getSize},				%get the total number of vertices
			receive
				{sizeIs,SizeOfTable}->ok		%thanks
			end,
			io:format("There are ~p People in the Table.~n",[SizeOfTable]),	%display on terminal
			Str=integer_to_list(SizeOfTable),								%turn to string
			wxStaticText:setLabel(ST2001, "There are "++ Str++" People in the Table."),	%display on UI
			loop(State);													%back to loop
	#wx{id = 118, event=#wxCommand{type = command_button_clicked}} ->
            %this message is sent when the Countdown button (ID 118) is clicked
			%%this button is used for turning the ETS table into ranked table
			io:format("It's too long for that now, do it manually.~n"),				%display on terminal
			wxStaticText:setLabel(ST2001, "It's too long for that now, try later."),	%display on UI
			loop(State);														%back to loop
		
     Msg ->
            %everything else ends up here
         io:format("loop default triggered: Got ~n ~p ~n", [Msg]),
         loop(State)
 
    end.
		
%%wait function used for sehif 3, prints the top N ranked people in the table		
wait(N,StaticText)-> 
		io:format("~n calculating tree for top ~p ~n",[N]),
		mapRed!{self(),eliteGroup,N},
		Str=integer_to_list(N),															%turn int to string
		wxStaticText:setLabel(StaticText, "Running, Fetching top  "++ Str++" authors."),	%display on UI
		ack(StaticText).

%%wait function used for sehif 1, prints the connection tree until desired depth for author
wait2(Author_val , Depth_val, StaticText)-> 
		Author=Author_val , Depth = list_to_integer(Depth_val),
		io:format("~n ~p ~p ~n",[Author,Depth]),
		mapRed!{self(),conTree,Author,Depth},
		wxStaticText:setLabel(StaticText, "Running"),
		ack(StaticText).



ack(StaticText)->
				   receive 
					   done -> 	wxStaticText:setLabel(StaticText, "Finish, open now GraphViz")
					end,ok.