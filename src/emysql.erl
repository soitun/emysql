%%%----------------------------------------------------------------------
%%% File    : emysql.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : Mysql access api.
%%% Created : 19 May 2009
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2012, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(emysql).

-author('ery.lee@gmail.com').

-include("emysql.hrl").

-export([start_link/0]).

-ifdef(use_specs).

-spec(conns/0 :: () -> list()).

-endif.

-export([info/0,
		add_conn/2,
		conns/0,
		insert/2,
        select/1,
        select/2,
        update/2,
        update/3,
        delete/1,
        delete/2,
        prepare/2,
        execute/1,
        execute/2,
        unprepare/1,
        sqlquery/1,
		sqlquery/2]).

-behavior(gen_server2).

-export([init/1,
		prioritise_call/3,
        handle_call/3,
		prioritise_cast/2,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(mysql_conn, {id, pid, load = 0, ref}).

-record(state, {}).

%% External exports
-export([encode/1,
	    encode/2,
        escape/1,
	    escape_like/1]).

start_link() ->
	gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

info() ->
	[{Conn#mysql_conn.id, Conn#mysql_conn.load} || Conn <- conns()].

conns() ->
	gen_server2:call(?MODULE, conns).

add_conn(Id, Pid) ->
	gen_server2:call(?MODULE, {add_conn, Id, Pid}).

insert(Tab, Record) when is_atom(Tab) ->
	sqlquery(encode_insert(Tab, Record)).

encode_insert(Tab, Record) ->
	{Fields, Values} = lists:unzip([{atom_to_list(F), encode(V)} 
		|| {F, V} <- Record]),
	["insert into ", atom_to_list(Tab), "(",
		 string:join(Fields, ","), ") values(",
		 string:join(Values, ","), ");"].

select(Select) ->
	sqlquery(encode_select(Select)).

select(Select, Load) ->
	sqlquery(encode_select(Select), Load).

encode_select(Tab) when is_atom(Tab) ->
	encode_select({Tab, ['*'], undefined});

encode_select({Tab, Fields}) when is_atom(Tab) 
	and is_list(Fields) ->
    encode_select({Tab, Fields, undefined});

encode_select({Tab, Where}) when is_atom(Tab) 
	and is_tuple(Where) ->
	encode_select({Tab, ['*'], Where});

encode_select({Tab, Fields, undefined}) when is_atom(Tab) 
	and is_list(Fields) ->
	["select ", encode_fields(Fields), " from ", atom_to_list(Tab), ";"];

encode_select({Tab, Fields, Where}) when is_atom(Tab) 
	and is_list(Fields) and is_tuple(Where) ->
	["select ", encode_fields(Fields), " from ",
	 atom_to_list(Tab), " where ", encode_where(Where), ";"].

encode_fields(Fields) ->
    string:join([atom_to_list(F) || F <- Fields], " ,").

update(Tab, Record) ->
	case proplists:get_value(id, Record) of 
    undefined ->
        {error, no_id_found};
    Id ->
        update(Tab, lists:keydelete(id, 1, Record), {id, Id})
	end.

update(Tab, Record, Where) ->
	Update = string:join([atom_to_list(F) ++ "=" ++ encode(V) || {F, V} <- Record], ","),
    Query = ["update ", atom_to_list(Tab), " set ", Update, " where ", encode_where(Where), ";"],
	sqlquery(Query).

delete(Tab) when is_atom(Tab) ->
	sqlquery(["delete from ", atom_to_list(Tab), ";"]).

delete(Tab, Id) when is_atom(Tab)
	and is_integer(Id) ->
    Query = ["delete from ", atom_to_list(Tab), 
			 " where ", encode_where({id, Id})],
	sqlquery(Query);

delete(Tab, Where) when is_atom(Tab)
	and is_tuple(Where) ->
    Query = ["delete from ", atom_to_list(Tab),
			 " where ", encode_where(Where)],
	sqlquery(Query).

sqlquery(Query) ->
	sqlquery(Query, 1).

sqlquery(Query, Load) -> 
	with_next_conn(fun(Conn) ->
		case catch mysql_to_odbc(emysql_conn:sqlquery(Conn, iolist_to_binary(Query))) of
		{selected, NewFields, Records} -> 
			{ok, to_tuple_records(NewFields, Records)};
		{error, Reason} -> 
			{error, Reason};
		Res ->
			Res
		end
	end, Load).

prepare(Name, Stmt) when is_list(Stmt) ->
	prepare(Name, list_to_binary(Stmt));

prepare(Name, Stmt) when is_binary(Stmt) ->
	with_all_conns(fun(Conn) ->
		emysql_conn:prepare(Conn, Name, Stmt)
	end).

execute(Name) ->
	execute(Name, []).

execute(Name, Params) ->
	with_next_conn(fun(Conn) ->
		case catch mysql_to_odbc(emysql_conn:execute(Conn, Name, Params)) of
		{selected, NewFields, Records} -> 
			{ok, to_tuple_records(NewFields, Records)};
		{error, Reason} -> 
			{error, Reason};
		Res ->
			Res
		end
	end, 1).

unprepare(Name) ->
	with_all_conns(fun(Conn) ->
		emysql_conn:unprepare(Conn, Name)
	end).

with_next_conn(Fun, Load) ->
	Conn = gen_server2:call(?MODULE, {next_conn, Load}),
	if
	Conn == undefined -> 
		{error, no_mysql_conn};
	true -> 
		Result = Fun(Conn#mysql_conn.pid),
		gen_server2:cast(?MODULE, {done, Conn#mysql_conn.id, Load}),
		Result
	end.

with_all_conns(Fun) ->
	[Fun(Conn#mysql_conn.pid) || Conn <- gen_server2:call(?MODULE, conns)].

%%--------------------------------------------------------------------
%% Function: init(Args) -> {ok, State} |
%%                         {ok, State, Timeout} |
%%                         ignore               |
%%                         {stop, Reason}
%% Description: Initiates the server
%%--------------------------------------------------------------------
init([]) ->
	ets:new(mysql_conn, [set, protected, named_table, {keypos, 2}]),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% Function: %% handle_call(Request, From, State) -> {reply, Reply, State} |
%%                                      {reply, Reply, State, Timeout} |
%%                                      {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, Reply, State} |
%%                                      {stop, Reason, State}
%% Description: Handling call messages
%%--------------------------------------------------------------------
prioritise_call({add_conn, _Id, _Pid}, _From, _State) ->
	10;
prioritise_call(conns, _From, _State) ->
	10;
prioritise_call({next_conn, _Load}, _From, _State) ->
	8;
prioritise_call(_Req, _From, _State) ->
	0.

handle_call({add_conn, Id, Pid}, _From, State) ->
	Ref = erlang:monitor(process, Pid),
	ets:insert(mysql_conn, #mysql_conn{id = Id, pid = Pid, ref = Ref}),
	{reply, ok, State};

handle_call({next_conn, Load}, _From, State) ->
	Conn = find_next_conn(ets:first(mysql_conn)),
	case Conn of
	undefined -> 
		undefined;
	_ -> 
		OldLoad = Conn#mysql_conn.load,
		NewConn = Conn#mysql_conn{load = OldLoad+Load},
		ets:insert(mysql_conn, NewConn)
	end,
	{reply, Conn, State};
	
handle_call(conns, _From, State) ->
	Conns = find_all_conns(ets:first(mysql_conn)),
	{reply, Conns, State};

handle_call(Req, From, State) ->
    gen_server:reply(From, {badcall, Req}),
    {stop, {badcall, Req}, State}.

%%--------------------------------------------------------------------
%% Function: handle_cast(Msg, State) -> {noreply, State} |
%%                                      {noreply, State, Timeout} |
%%                                      {stop, Reason, State}
%% Description: Handling cast messages
%%--------------------------------------------------------------------
prioritise_cast(_Msg, _State) ->
	0.

handle_cast({done, ConnId, Load}, State) ->
	case ets:lookup(mysql_conn, ConnId) of
	[Conn] -> 
		AllLoad = Conn#mysql_conn.load,	
		ets:insert(mysql_conn, Conn#mysql_conn{load = AllLoad-Load});
	[] ->
		error_logger:error_msg("cannot find conn with id: ~p", [ConnId])
	end,
	{noreply, State};

handle_cast(Msg, State) ->
    {stop, {badcast, Msg}, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({'DOWN', MonitorRef, _Type, _Object, _Info}, State) ->
	Matches = ets:match(mysql_conn, {mysql_conn, '$1', '_', '_', MonitorRef}),
	[ets:delete(mysql_conn, Id) || [Id] <- Matches],
	{noreply, State};

handle_info(Info, State) ->
    {stop, {badinfo, Info}, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.
%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

find_next_conn(Key) ->
	find_next_conn(Key, undefined, -1).

find_next_conn('$end_of_table', Conn, _Load) ->
	Conn;
find_next_conn(Key, Conn, Load) ->
	[#mysql_conn{load = ThisLoad} = ThisConn] 
		= ets:lookup(mysql_conn, Key),
	NextKey = ets:next(mysql_conn, Key),
	if
	(Load == -1) or (ThisLoad =< Load) -> 
		find_next_conn(NextKey, ThisConn, ThisLoad);
	true ->
		find_next_conn(NextKey, Conn, Load)
	end.

find_all_conns(Key) ->
	find_all_conns(Key, []).

find_all_conns('$end_of_table', Conns) ->
	Conns;

find_all_conns(Key, Conns) ->
	[Conn] = ets:lookup(mysql_conn, Key),
	find_all_conns(ets:next(mysql_conn, Key), [Conn|Conns]).

%% Convert MySQL query result to Erlang ODBC result formalism
mysql_to_odbc({updated, #mysql_result{affectedrows=AffectedRows, insert_id = InsertId} = _MySQLRes}) ->
    {updated, {AffectedRows, InsertId}};

mysql_to_odbc({data, #mysql_result{fieldinfo = FieldInfo, rows=AllRows} = _MySQLRes}) ->
    mysql_item_to_odbc(FieldInfo, AllRows);

mysql_to_odbc({error, MySQLRes}) when is_list(MySQLRes) ->
    {error, MySQLRes};

mysql_to_odbc({error, #mysql_result{error=Reason} = _MySQLRes}) ->
    {error, Reason};

mysql_to_odbc({error, Reason}) ->
    {error, Reason}.

%% When tabular data is returned, convert it to the ODBC formalism
mysql_item_to_odbc(Columns, Recs) ->
    %% For now, there is a bug and we do not get the correct value from MySQL
    %% module:
    {selected,
     [element(2, Column) || Column <- Columns],
     [list_to_tuple(Rec) || Rec <- Recs]}.

%%internal functions
encode_where({'and', L, R}) ->
	encode_where(L) ++ " and " ++ encode_where(R);

encode_where({'or', L, R}) ->
	encode_where(L) ++ " or " ++ encode_where(R);

encode_where({like, Field, Value}) ->	
	atom_to_list(Field) ++ " like " ++ encode(Value);

encode_where({'<', Field, Value}) ->	
	atom_to_list(Field) ++ " < " ++ encode(Value);

encode_where({'>', Field, Value}) ->	
	atom_to_list(Field) ++ " > " ++ encode(Value);

encode_where({'in', Field, Values}) ->	
	InStr = string:join([encode(Value) || Value <- Values], ","),
	atom_to_list(Field) ++ " in (" ++ InStr ++ ")";

encode_where({Field, Value}) ->
	atom_to_list(Field) ++ " = " ++ encode(Value).

to_tuple_records(_Fields, []) ->
	[];

to_tuple_records(Fields, Records) ->
	[to_tuple_record(Fields, tuple_to_list(Record)) || Record <- Records].
	
to_tuple_record(Fields, Record) when length(Fields) == length(Record) ->
	to_tuple_record(Fields, Record, []).

to_tuple_record([], [], Acc) ->
	Acc;

to_tuple_record([_F|FT], [undefined|VT], Acc) ->
	to_tuple_record(FT, VT, Acc);

to_tuple_record([F|FT], [V|VT], Acc) ->
	to_tuple_record(FT, VT, [{list_to_atom(binary_to_list(F)), V} | Acc]).

%% Escape character that will confuse an SQL engine
%% Percent and underscore only need to be escaped for pattern matching like
%% statement
escape_like(S) when is_list(S) ->
    [escape_like(C) || C <- S];
escape_like($%) -> "\\%";
escape_like($_) -> "\\_";
escape_like(C)  -> escape(C).

%% Escape character that will confuse an SQL engine
escape(S) when is_list(S) ->
	[escape(C) || C <- S];
%% Characters to escape
escape($\0) -> "\\0";
escape($\n) -> "\\n";
escape($\t) -> "\\t";
escape($\b) -> "\\b";
escape($\r) -> "\\r";
escape($')  -> "\\'";
escape($")  -> "\\\"";
escape($\\) -> "\\\\";
escape(C)   -> C.

encode(Val) ->
    encode(Val, false).
encode(Val, false) when Val == undefined; Val == null ->
    "null";
encode(Val, true) when Val == undefined; Val == null ->
    <<"null">>;
encode(Val, false) when is_binary(Val) ->
    binary_to_list(quote(Val));
encode(Val, true) when is_binary(Val) ->
    quote(Val);
encode(Val, true) ->
    list_to_binary(encode(Val,false));
encode(Val, false) when is_atom(Val) ->
    quote(atom_to_list(Val));
encode(Val, false) when is_list(Val) ->
    quote(Val);
encode(Val, false) when is_integer(Val) ->
    integer_to_list(Val);
encode(Val, false) when is_float(Val) ->
    [Res] = io_lib:format("~w", [Val]),
    Res;
encode({datetime, Val}, AsBinary) ->
    encode(Val, AsBinary);
encode({{Year, Month, Day}, {Hour, Minute, Second}}, false) ->
    Res = two_digits([Year, Month, Day, Hour, Minute, Second]),
    lists:flatten(Res);
encode({TimeType, Val}, AsBinary)
  when TimeType == 'date';
       TimeType == 'time' ->
    encode(Val, AsBinary);
encode({Time1, Time2, Time3}, false) ->
    Res = two_digits([Time1, Time2, Time3]),
    lists:flatten(Res);
encode(Val, _AsBinary) ->
    {error, {unrecognized_value, Val}}.

two_digits(Nums) when is_list(Nums) ->
    [two_digits(Num) || Num <- Nums];
two_digits(Num) ->
    [Str] = io_lib:format("~b", [Num]),
    case length(Str) of
	1 -> [$0 | Str];
	_ -> Str
    end.

%%  Quote a string or binary value so that it can be included safely in a
%%  MySQL query.
quote(String) when is_list(String) ->
    [39 | lists:reverse([39 | quote(String, [])])];	%% 39 is $'
quote(Bin) when is_binary(Bin) ->
    list_to_binary(quote(binary_to_list(Bin))).

quote([], Acc) ->
    Acc;
quote([0 | Rest], Acc) ->
    quote(Rest, [$0, $\\ | Acc]);
quote([10 | Rest], Acc) ->
    quote(Rest, [$n, $\\ | Acc]);
quote([13 | Rest], Acc) ->
    quote(Rest, [$r, $\\ | Acc]);
quote([$\\ | Rest], Acc) ->
    quote(Rest, [$\\ , $\\ | Acc]);
quote([39 | Rest], Acc) ->		%% 39 is $'
    quote(Rest, [39, $\\ | Acc]);	%% 39 is $'
quote([34 | Rest], Acc) ->		%% 34 is $"
    quote(Rest, [34, $\\ | Acc]);	%% 34 is $"
quote([26 | Rest], Acc) ->
    quote(Rest, [$Z, $\\ | Acc]);
quote([C | Rest], Acc) ->
    quote(Rest, [C | Acc]).

