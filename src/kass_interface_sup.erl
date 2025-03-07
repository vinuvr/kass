%%%-------------------------------------------------------------------
%% @doc dbmgr interface supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(kass_interface_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
  SupFlags = #{strategy => one_for_one,
               intensity => 10,
               period => 10},
  ChildSpecs = [
               
               
               %,#{id => kass_kafka_producer, start => {kass_kafka_producer, start_link, []}}
               %,#{id => kass_kafka_consumer, start => {kass_kafka_consumer, start_link, []}}
               #{id => kass_if_cassandra_node_sofo, start => {kass_if_cassandra_node_sofo,
                                                                 start_link, []}, type => supervisor}
               ,#{id => kass_if_cassandra_nodes, start => {kass_if_cassandra_nodes, start_link, []}}
               ,#{id => kass_cassandra, start => {kass_cassandra, start_link, []}}
               ,#{id => dbmigration, start => {dbmigration, start_link, []}}

               ],
  {ok, {SupFlags, ChildSpecs}}.

%% internal functions
