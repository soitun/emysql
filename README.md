Erlang MySQL client

## Compile with Rebar

compile rebar:

* git clone https://github.com/basho/rebar
* cd rebar
* ./bootstrap

compile emysql

* cp rebar emysql/
* cd emysql
*./rebar compile

## Select API

* emsysql:select(tab).
* emysql:select({tab, [col1,col2]}).
* emysql:select({tab, [col1, col2], {id,1}}).
* emysql:select(Query, Load).

## Update API

* emysql:update(tab, [{Field1, Val}, {Field2, Val2}], {id, 1}).

## Insert API

* emsyql:insert(tab, [{Field1, Val}, {Field2, Val2}]).

## Delete API

* emsyql:delete(tab, {name, Name}]).

## Query API

* emysql:sqlquery("select * from tab;").

## Prepare API

* emysql:prepare(find_with_id, "select * from tab where id = ?;").
* emysql:execute(find_with_id, [Id]).
* emysql:unprepare(find_with_id).

## MySQL Client Protocal

* http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol
