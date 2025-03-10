%%%-------------------------------------------------------------------
%%% @author VINU
%%% @copyright (C) 2024, Netstratum Technologies Pvt Ltd
%%% @doc
%%%
%%% @end
%%% Created : 2024-01-21 16:02:13.977469
%%%-------------------------------------------------------------------
-module(kass_cassandra).

-behaviour(gen_server).

%% API
-export([start_link/0
         ,query/1
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

query(Query) ->
  lager:info("calling the query ~p",[Query]),
  gen_server:call(?MODULE, {query, Query}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
  {ok, #{}}.

handle_call({query, Query},  _From, State) ->
  lager:info("received the query"),
  Reply = execute_query(Query),
  {reply, Reply, State};
handle_call(Request, _From, State) ->
  lager:info("the req ~p, state ~p",[Request, State]),
  Reply = ok,
  {reply, Reply, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(retry, State) ->
  lager:info("received the retry"),
  %StateU = retry_connection(State),
  {noreply, State};
handle_info(check_status, State) ->
  lager:info("checking status"),
  %{ok, StateU} = do_check_status(State),
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%connect_to_cassandra(Clients) ->
%  Clients = [#{client1 => [{"134.195.41.90", 9042}, {keyspace, "tutorialspoint"}, {maps, true}]}
%             ,#{client2 => [{"134.195.41.90", 9043}, {keyspace, "tutorialspoint"}, {maps, true}]],
%  Clients = [{"134.195.41.90", 9042}, [{keyspace, "tutorialspoint"}, {maps, true}]
%             ,{"134.195.41.90", 9043}, [{keyspace, "tutorialspoint"}, {maps, true}]],
%  Out = lists:foldl(
%          fun([IPInfo, Spec], ClientInfo, #{s_clients := SC, f_clients := FC} = Ac) ->
%              case cqerl:get_client(IPInfo, Spec) of
%                {ok, Client} ->
%                  Out = [Client, IpInfo, Spec],
%                  #{s_clients => [Out|SC], f_clients => FC};
%                Other ->
%                  lager:info("failed to connect to cassandra ~p",[Other]),
%                  #{s_clients => SC, f_clients => [ClientInfo|Fc]}
%              end, #{s_clients => [], f_clients => []}, Clients),
%  erlang:send_after(1000, self(), check_status),
%  {ok, Out}.
%  {ok, Clients}.

%do_check_status(#{f_clients := [], s_clients := _SClients}) ->
%  lager:info("no failed clients found checking connected clients"),
%  ok.
%
%  connect_to_cassandra(
%do_check_status(#{s_clients := SClients, f_clients := FClients}) ->


%%%  lists:foldl(
%%%    fun(CNodeInfo, #{failed_clients := FC} = CassandraMap}) ->
%%%        [ClientInfo] = maps:values(CNodeInfo),
%%%        [ClientN] = maps:keys(CNodeInfo),
%%%        case cqerl:get_client(ClientInfo) of
%%%          {ok, Client} -> maps:merge(CassandraMap, #{ClientN => Client});
%%%          Other ->
%%%            lager:info("failed to connect to cassandra ~p",[Other]),
%%%            map:merge(CassandraMap, #{failed_clients => [ClientInfo|FC]})
%%%        end
%%%    end, #{failed_clients => []}, Clients),

%  {ok, Client1} = cqerl:get_client({"134.195.41.90", 9042}, [{keyspace, "tutorialspoint"}
                                                             %,{maps, true}
%                                                             ]),

%  application:set_env([
%                       {cqerl, [
%                                {cassandra_nodes, [ { "134.195.41.90", 9042 } ]},
%                                {ssl, false},
%                                {keyspace, "vinu"},
%                                {auth, {cqerl_auth_plain_handler, [ {"test", "aaa"} ]}}
%                               ]}
%                      ]),
%  case  cqerl:get_client({}) of
%    {ok, {_, _} = Client} ->
%      lager:info("client info is ~p",[Client]),
%      {ok, #{client => Client}};
%    Other ->
%      lager:info("failed to connect to casssandra ~p",[Other]),
%      erlang:send_after(2000, self(), retry),
%      {ok, #{}}
%  end.
%
%retry_connection(
%retry_connection(State) ->
%  case  cqerl:get_client({}) of
%    {ok, {_, _} = Client } ->
%      lager:info("client info is ~p",[Client]),
%      {ok, State#{client => Client}};
%    Other ->
%      lager:info("failed to connect to casssandra ~p",[Other]),
%      erlang:send_after(2000, self(), retry),
%      State
%  end.

execute_query(Query) ->
  case kass_if_cassandra_nodes:get_random_active_node() of
    {ok, CSNode} ->
      %lager:info("cassandra node is ~p",[CSNode]),
      kass_if_cassandra_node:query(CSNode, Query);
    {error,<<"None of cassandra are connected">>} = Res ->
      lager:info("no cassandra nodes connected"),
      Res
  end.

% cqerl:run_query(Client, Query).
