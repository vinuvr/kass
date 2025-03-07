%%%-------------------------------------------------------------------
%% @doc kass public API
%% @end
%%%-------------------------------------------------------------------

-module(kass_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    kass_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
