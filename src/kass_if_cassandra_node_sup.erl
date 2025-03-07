%%%-------------------------------------------------------------------
%%% @author VINU
%%% @copyright (C) 2024, VINU
%%% @doc
%%%
%%% @end
%%% Created : 2024-07-10 19:20:35.133120
%%%-------------------------------------------------------------------
-module(kass_if_cassandra_node_sup).

-behaviour(supervisor).

%% API
-export([start_link/1,
         server_name/1
        ]).

%% Supervisor callbacks
-export([init/1]).

-define(CHILD(Module, Name), #{id => Module,
                               start => {Module, start_link, [Name]}
                              }).
-define(CHILD_SUP(Module, Name), #{id => Module,
                                   start => {Module, start_link, [Name]},
                                   type => supervisor
                                  }).

%%====================================================================
%% API functions
%%====================================================================

start_link(CSNode) ->
  lager:info("Going to stat csnode ~p",[CSNode]),
  ServerName = server_name(CSNode),
  lager:info("starting supervisor module: ~p",[ServerName]),
  supervisor:start_link({local, ServerName}, ?MODULE, [CSNode]).

server_name([{IP, Port}, _Spec]) ->
  list_to_atom(kass_utils:to(list, ?MODULE) ++ "_" ++ kass_utils:to(list, IP)
               ++ ":" ++ kass_utils:to(list, Port)).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: #{id => Id, start => {M, F, A}}
%% Optional keys are restart, shutdown, type, modules.
%% Before OTP 18 tuples must be used to specify a child. e.g.
%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([CSNode]) ->
  SupervisorFlags = #{strategy => one_for_one,
                      intensity => 20,
                      period => 60},
  ChildSpecs = [
                ?CHILD(kass_if_cassandra_node, CSNode)
               % ?CHILD(kass_if_fs_cassandra, CSNode),
               % ?CHILD(kass_if_cassandra_out, CSNode)
               ],
  {ok, {SupervisorFlags, ChildSpecs} }.

%%====================================================================
%% Internal functions
%%====================================================================
