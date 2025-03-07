%%%-------------------------------------------------------------------
%%% @author VINU
%%% @copyright (C) 2024, shiva
%%% @doc
%%%
%%% @end
%%% Created : 2024-07-10 19:27:37.516654
%%%-------------------------------------------------------------------
-module(kass_if_cassandra_nodes).

-behaviour(gen_server).

%% API
-export([start_link/0,
         add/1,
         remove/1,
         get_random_active_node/0,
         get_nodes/0,
         get_active_nodes/0
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

add(CSNode) ->
  gen_server:call(?SERVER, {add, kass_utils:to(binary, CSNode)}).

remove(CSNode) ->
  gen_server:call(?SERVER, {remove, kass_utils:to(binary, CSNode)}).

get_random_active_node() ->
  gen_server:call(?SERVER, get_random_active_node).

get_nodes() ->
  gen_server:call(?SERVER, get_nodes).

get_active_nodes() ->
  gen_server:call(?SERVER, get_active_nodes).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
  process_flag(trap_exit, true),
  {ok, #{}, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_call({add, CSNode}, _From, #{cs_nodes := CSNodes} = State) ->
  case lists:member(CSNode, CSNodes) of
    true ->
      {reply, {error, <<"Node already exists">>}, State};
    false ->
      start_cs_node_tree(CSNode),
      {reply, ok, State#{cs_nodes => [CSNode|CSNodes]}}
  end;
handle_call({remove, CSNode},  _From, #{cs_nodes := CSNodes} = State) ->
  case lists:member(CSNode, CSNodes) of
    false ->
      {reply, {error, <<"Node not found.">>}, State};
    true ->
      stop_cs_node_tree(CSNode),
      {reply, ok, State#{cs_nodes => lists:delete(CSNode, CSNodes)}}
  end;

handle_call(get_random_active_node, _From, #{cs_nodes := CSNodes} = State) ->
  Response = handle_get_random_active_node(CSNodes),
  {reply, Response, State};
handle_call(get_nodes, _From, #{cs_nodes := CSNodes} = State) ->
  {reply, {ok, CSNodes}, State};
handle_call(get_active_nodes, _From, #{cs_nodes := CSNodes} = State) ->
  ActiveNodes = get_active_nodes(CSNodes),
  {reply, {ok, ActiveNodes}, State};
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(timeout, _State) ->
  StateU = start_cs_nodes(),
  {noreply, StateU};
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(Reason, State) ->
  lager:info("Terminating with state:~p ,  Reason:~p", [State, Reason]),
  stop_cs_nodes(State),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
start_cs_nodes() ->
  lager:info("Starting cassandra nodes supervisors..."),
  %CSNodesStr = application:get_env(pbx, freeswitch_nodes, ""),
  CSNodes = [[{"134.195.41.183", 9042}, [{keyspace, "tutorialspoint"}, {maps, true}]]
             %,[{"108.101.10.223", 9042}, [{keyspace, "tutorialspoint"}, {maps, true}]]
            ],

%  [[{"134.195.41.90", 9042}, [{keyspace, "tutorialspoint"}, {maps, true}]]
%            ,[{"134.195.41.90", 9043}, [{keyspace, "tutorialspoint"}, {maps, true}]]],
  lists:foreach(fun start_cs_node_tree/1, CSNodes),
  lager:info("the csnode state ~p",[CSNodes]),
  #{cs_nodes => CSNodes}.

stop_cs_nodes(#{cs_nodes := CSNodes}) ->
  lager:info("stopping fs nodes supervisors..."),
  lists:foreach(fun stop_cs_node_tree/1, CSNodes).

start_cs_node_tree(CSNode) ->
  lager:info("Connecting cs node ~p", [CSNode]),
  kass_if_cassandra_node_sofo:start(CSNode).

stop_cs_node_tree(CSNode) ->
  lager:info("Stopping fs node ~p", [CSNode]),
  kass_if_cassandra_node_sofo:stop(CSNode).

handle_get_random_active_node([]) ->
  {error, <<"None of cassandra are connected">>};
handle_get_random_active_node([CSNode|Rest]) ->
  case kass_if_cassandra_node:get_node_state(CSNode) of
    {ok, up} ->
      {ok, CSNode};
    _ ->
      handle_get_random_active_node(Rest)
  end.

get_active_nodes(CSNodes) ->
  lists:filter(
    fun(CSNode) ->
        case kass_if_cassandra_node:get_node_state(CSNode) of
          {ok, up} ->
            true;
          _ ->
            false
        end
    end, CSNodes).
