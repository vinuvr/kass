%%%-------------------------------------------------------------------
%% @doc kass top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(kass_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

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
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [#{id => kass_uuid, start => {kass_uuid, start_link, []}}
                   ,#{id => kass_interface_sup, start => {kass_interface_sup, start_link, []}}
                %   ,#{id => kass_if_cassandra_node_sofo, start => {kass_if_cassandra_node_sofo, start_link, []}} 
                %   ,#{id => kass_if_cassandra_nodes, start => {kass_if_cassandra_nodes, start_link, []}}
                %   ,#{id=> kass_cassandra, start => {kass_cassandra, start_link, []}}
                %   ,#{id => kass, start => {kass, start_link, []}} 
                  %%kass_cassandra:start_link().
                  %%kass_if_cassandra_nodes:start_link().

                   %%kass_if_cassandra_node_sofo:start_link().    

],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
