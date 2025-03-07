%%%%%%%%%%%%%%%%% only for migration remove after use%%%%%%%%%%%%%%%%%%%



-module(dbmigration_starter).

-behaviour(gen_server).

-export([start_link/0, stop/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
  gen_server:call(?MODULE, stop).



init([]) ->
  {ok, #{},0 }.

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
  Reply = {error, unknown_request},
  {reply, Reply, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(timeout, State) ->
  catch start_day_by_day_migration(),
  {noreply, State};
handle_info(completed, State) ->
  lager:info("completed the migration"),
  {stop, normal, State};
handle_info({next_day_migration, StartDate, EndDate, PEndDate}, State) ->
  lager:info("got the next day migration"),
  catch do_start_migration(StartDate, EndDate, PEndDate ),
  {noreply, State};
handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


start_day_by_day_migration() ->
  lager:info("start_month_by_month_migration"),
  StartDate = 1725930001000, %1690506001000, %% change based on the server
  PEndDate = StartDate + 86400000,
  EndDate = erlang:system_time(millisecond),
  lager:info("start_date ~p end_date ~p", [StartDate, EndDate]),
  do_start_migration(StartDate, EndDate, PEndDate).

do_start_migration(StartDate, EndDate, PEndDate) when PEndDate > EndDate ->
  %% To get data upto that day and migrate
  Data = hoolva_chat_messages:get_chat(#{a_ctime => {range, StartDate, EndDate}}),
  R = spawn(fun() -> dbmigration:start_migration(lists:usort(Data)) end),
  lager:info("started migraton pid ~p lastdate", [R]),
  self() ! completed;
do_start_migration(StartDate, EndDate, PEndDate) ->
  lager:info("start_date ~p pend_date ~p", [StartDate, PEndDate]),
  Data = hoolva_chat_messages:get_chat(#{a_ctime => {range, StartDate, PEndDate}}),
  R = spawn(fun() -> dbmigration:start_migration(lists:usort(Data)) end),
  {{Y, M,D }, _} = calendar:system_time_to_universal_time(StartDate, millisecond),
  DATE = to_list(D) ++ "-" ++ to_list(M) ++ "-" ++ to_list(Y),
  lager:info("started migraton pid ~p date ~p", [R, DATE]),
  erlang:send_after(3000, self(), {next_day_migration, PEndDate+1, EndDate, PEndDate + 86400000}),
  ok.

%do_start_migration(PEndDate+1, EndDate, PEndDate + 86400000).

to_list(A) ->
  kass_utils:to(list, A).







%hoolva_chat_messages:get_chat(#{a_ctime => {range, 1740125481016, erlang:system_time(millisecond)}})).










