%%%----------------------------------------------------------------------
%%% File    : mysql_app.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : mysql driver application
%%% Created : 21 May 2009
%%% Updated : 11 Jan 2010 
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2007-2010, www.opengoss.com 
%%%----------------------------------------------------------------------
-module(mysql_app).

-author('ery.lee@gmail.com').

-behavior(application).

-export([start/0, start/2, stop/1]).

start() -> 
    application:start(crypto),
	elog:init(5, "mysql.log"),
    io:format("starting mysql..."),
	application:start(mysql).

start(normal, _Args) ->
	mysql_sup:start_link(application:get_all_env()).

stop(_) ->
	ok.

