%%%-------------------------------------------------------------------
%%% @author VINU
%%% @copyright (C) 2024, VINU
%%% @doc
%%%
%%% @end
%%% Created : 2024-07-10 16:48:20.791599
%%%-------------------------------------------------------------------
-module(kass_if_cassandra_node).

-behaviour(gen_server).

%% API
-export([start_link/1,
         get_node_state/1,
         query/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(RETRY_INTERVAL, 10). % 5 sec

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
start_link(CSNode) ->
  lager:info("Going ot start the cassandra node ~p",[CSNode]),
  gen_server:start_link({local, server_name(CSNode)}, ?MODULE, [CSNode], []).

get_node_state(CSNode) ->
  gen_server:call(server_name(CSNode), <<"get_node_state">>).

query(CSNode, Query) ->
  lager:info("calling the query ~p",[Query]),
  gen_server:call(server_name(CSNode), {<<"query">>, Query}).
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
init([CSNode]) ->
  process_flag(trap_exit, true),
  lager:md([{id, server_name(CSNode)}]),
  lager:info("starting cs node ~p", [CSNode]),
  start_ping_timer(),
  {ok, #{cs_node => kass_utils:to(atom, server_name(CSNode)), connection => down
         ,client_info => CSNode}}.

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
handle_call(<<"get_node_state">>, _From, #{connection := up} = State) ->
  lager:info("the state ~p",[State]),
  {reply, {ok, up}, State};
handle_call(<<"get_node_state">>, _From, State) ->
  lager:info("the state is ~p",[State]),
  {reply, {error, <<"connection_down">>}, State};
handle_call({<<"query">>, Query}, _From, State) ->
 %lager:info("Got query  ~p",[Query]),
 Reply = execute_query(State, Query),
 {reply, Reply, State};
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
handle_info(ping_node, #{cs_node := undefined} = State) ->
  lager:info("CSNode is not configured."),
  start_ping_timer(),
  {noreply, State};
handle_info(ping_node, #{connection := down} = State) ->
  lager:info("node is down trying to reconnect...."),
  NewState = do_connect_cs_node(State),
  {noreply, NewState};
handle_info({'nodedown', CSNode}, #{cs_node := CSNode} = State) ->
  lager:info("CS Node ~p went down. state ~p", [CSNode, State]),
  %% rmv from active node
  NewState = do_connect_cs_node(State),
  {noreply, NewState};
handle_info({'EXIT', _,connection_closed}, #{cs_node := CSNode} = State) ->
  lager:info("Cs Node ~p went down, ~p",[CSNode, State]),
  start_ping_timer(),
  {noreply, State#{connection => down}};
handle_info(Info, State) ->
  lager:info("Unknown message. Info:~p, State:~p", [Info, State]),
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
  lager:info("terminating with state:~p, reason:~p", [State, Reason]),
  'ok'.
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

start_ping_timer() ->
  erlang:send_after(?RETRY_INTERVAL*1000, self(), ping_node).

do_connect_cs_node(#{client_info := ClientInfo} = State) ->
  lager:info("Going to connect cs node ~p with state ~p",[ClientInfo, State]),
  [IPInfo, Spec] = ClientInfo,
  case catch cqerl:get_client(IPInfo, Spec) of
    {ok, Client} ->
      lager:info("client info is ~p",[Client]),
      {ClientPid, _} = Client,
      link(ClientPid),
      State#{client => Client, connection => up};
    {'EXIT', Reason} ->
      lager:info("failed to connect to cs client ~p reason ~p",[ClientInfo, Reason]),
      start_ping_timer(),
      State#{connection => down};
    Other ->
      lager:info("Failed to connect to cassandra ~p",[Other]),
      start_ping_timer(),
      State#{connection => down}
  end.

server_name([{IP, Port}, _Spec] = Info) ->
  lager:info("cs node info is ~p",[Info]),
  CSNodeBin = kass_utils:to(
                binary, kass_utils:to(list, IP) ++ ":" ++
                kass_utils:to(list, Port)),
  kass_utils:to(atom, <<"cs_node_", CSNodeBin/binary >>);
server_name(Other) ->
  lager:error("Got unknown server name ~p",[Other]),
  Other.

execute_query(#{client := Client, connection := up}, Query) ->
  cqerl:run_query(Client, kass_utils:to(list, Query));
execute_query(_, _) ->
  {error, <<"no cs node found">>}.
