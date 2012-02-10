%%%----------------------------------------------------------------------
%%% File    : mysql_sup.erl
%%% Author  : Ery Lee
%%% Purpose : Mysql driver supervisor
%%% Created : 21 May 2009 
%%% Updated : 11 Jan 2010 
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2007-2010, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(mysql_sup).

-author('ery.lee@gmail.com').

-behavior(supervisor).

%% API
-export([start_link/1, init/1]).

start_link(Opts) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, Opts).  

init(Opts) ->
    PoolSize = proplists:get_value(pool_size, Opts, 4),
    Clients = [begin 
        Id = "mysql_conn_" ++ integer_to_list(I),
        {Id, {mysql_conn, start_link, [Opts]}, permanent, 10, worker, [mysql_conn]}
    end || I <- lists:seq(1, PoolSize)], 
    {ok, {{one_for_one, 10, 100}, Clients}}.
