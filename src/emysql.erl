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

-import(extbif, [to_binary/1]).

-export([insert/2, 
        select/1, 
        select/2, 
        select/3, 
        update/2, 
        update/3, 
        delete/1, 
        delete/2,
        prepare/2,
        execute/1,
        execute/2,
        unprepare/1,
        sql_query/1]).

%% External exports
-export([encode/1,
	    encode/2,
        escape/1,
	    escape_like/1]).

insert(Tab0, Record) ->
    Tab = atom_to_list(Tab0),
	Fields = string:join([atom_to_list(F) || {F, _} <- Record], ","),
	Values = string:join([encode(V) || {_, V} <- Record], ","),
    Query = ["insert into ", Tab, "(", Fields, ") values(", Values, ");"],
    %?INFO("~p", [list_to_binary(Query)]),
    sql_query(list_to_binary(Query)).

select(Tab) ->
	select(Tab, ['*'], undefined).

select(Tab, Fields) when is_list(Fields) ->
    select(Tab, Fields, undefined);

select(Tab, Where) when is_tuple(Where) ->
	select(Tab, ['*'], Where).

select(Tab0, Fields0, Where0) ->
    Tab = atom_to_list(Tab0),
    Fields = string:join([atom_to_list(F) || F <- Fields0], " ,"),
	Query = case Where0 of
    undefined -> 
        ["select ", Fields, " from ", Tab, ";"];
    Where0 -> 
        Where = encode_where(Where0),
        ["select ", Fields, " from ", Tab, " where ", Where, ";"]
	end,
	sql_query(list_to_binary(Query)).

update(Tab, Record) ->
	case dataset:get_value(id, Record) of 
    {value, Id} ->
        update(Tab, dataset:key_delete(id, Record), {id, Id});
    {false, _} ->
        {error, no_id_found}
	end.

update(Tab0, Record, Where0) ->
    Tab = atom_to_list(Tab0),
    Where = encode_where(Where0),
	Update = string:join([atom_to_list(F) ++ "=" ++ encode(V) || {F, V} <- Record], ","),
    Query = ["update ", Tab, " set ", Update, " where ", Where, ";"],
	sql_query(list_to_binary(Query)).

delete(Tab) ->
    Query = ["delete from ", atom_to_list(Tab), ";"],
	sql_query(list_to_binary(Query)).

delete(Tab0, Id) when is_integer(Id) ->
    Tab = atom_to_list(Tab0),
    Where = encode_where({id, Id}),
    Query = ["delete from ", Tab, " where ", Where],
	sql_query(list_to_binary(Query));

delete(Tab0, Where0) when is_tuple(Where0) ->
    Tab = atom_to_list(Tab0),
    Where = encode_where(Where0),
    Query = ["delete from ", Tab, " where ", Where],
	sql_query(list_to_binary(Query)).

sql_query(Query) ->
	case catch mysql_to_odbc(emysql_conn:sql_query(to_binary(Query))) of
    {selected, NewFields, Records} -> 
        {ok, to_tuple_records(NewFields, Records)};
    {error, Reason} -> 
        {error, Reason};
    Res ->
        Res
	end.

prepare(Name, Stmt) ->
    emysql_conn:prepare(Name, Stmt).

execute(Name) ->
    emysql_conn:execute(Name, []).

execute(Name, Params) ->
    emysql_conn:execute(Name, Params).

unprepare(Name) ->
    emysql_conn:unprepare(Name).

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
