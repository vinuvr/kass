%%%-------------------------------------------------------------------
%%% @author VINU
%%% @copyright (C) 2024, shiva
%%% @doc
%%%
%%% @end
%%% Created : 2024-07-10 19:06:57.124794
%%%-------------------------------------------------------------------
-module(kass_if_cassandra_node_sofo).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start/1,
         stop/1
        ]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start(CSNode) ->
  supervisor:start_child(?MODULE, [CSNode]).

stop(CSNode) ->
  ServerName = kass_if_cassandra_node_sup:server_name(CSNode),
  Pid = whereis(ServerName),
  supervisor:terminate_child(?MODULE, Pid).
%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
  lager:info("kass_if_cassandra_node_sofo started"),
  SupervisorFlags = #{strategy => simple_one_for_one,
                      intensity => 20,
                      period => 60
                     },
  DynamicChildSpec = #{id=> kass_if_cassandra_node_sup,
                       start => {kass_if_cassandra_node_sup, start_link,[]},
                       type => supervisor,
                       restart => transient
                      },
  {ok, {SupervisorFlags, [DynamicChildSpec]}}.
%%%===================================================================
%%% Internal functions
%%%===================================================================



