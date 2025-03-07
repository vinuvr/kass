-module(kass_uuid).  % Replace my_gen_server with your module name
-behaviour(gen_server).

%% API
-export([start_link/0, stop/0]). % Add other API functions as needed

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-export([]).


-export([time_uuid/0
         ,time_uuid/1]).
%%===================================================================
%%% API

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []). % Adjust args as needed

stop() ->
  gen_server:call(?MODULE, stop).

time_uuid() ->
  gen_server:call(?MODULE, time_uuid).

time_uuid(Time) when is_integer(Time) ->
  gen_server:call(?MODULE, {time_uuid, Time}).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->  % Initialization; replace [] with arguments if necessary
  {ok, #{}, 0}.  % Replace #state{} with your initial state

handle_call({time_uuid, Time}, _From, State) -> 
  Reply = create_time_uuid_from_time(Time, State),
  {reply, Reply, State};
handle_call(time_uuid, _From, State) ->
  {Reply, NewState} = create_time_uuid(State),
  {reply, Reply, NewState};
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}; % Graceful shutdown
handle_call(_Request, _From, State) ->
  Reply = {error, unknown_request},  % Handle other calls here
  {reply, Reply, State}.


handle_cast(_Msg, State) -> % Handle asynchronous messages
  {noreply, State}.

handle_info(timeout, State) ->
  StateU = create_time_uuid(State),
  {noreply, StateU};
handle_info(_Info, State) -> % Handle timeouts, other messages
  {noreply, State}.

terminate(_Reason, _State) ->  % Cleanup before termination
  ok.

code_change(_OldVsn, State, _Extra) ->  % For hot code swapping
  {ok, State}.

%%%===================================================================
%%% Internal functions and state definition
%%%===================================================================

create_time_uuid(State) ->
  lager:info("qqq state ~p",[State]),
  case maps:size(State)> 0 of
    true ->
      State1 = maps:get(uuid_state, State),
      {UUID, NewState} = uuid:get_v1(State1),
      {list_to_binary(uuid:uuid_to_string(UUID))
       ,#{uuid_state => NewState}};
    false ->
     StateU = uuid:new(self()),
     {_UUID, NewState} = uuid:get_v1(StateU),
     #{uuid_state => NewState}
end.

create_time_uuid_from_time(Time, State) ->
  NodeId = element(3, maps:get(uuid_state, State)),
  % Calculate the timestamp in 100-nanosecond intervals since the UUID epoch (1582-10-15)
% Unix epoch (1970-01-01) is 122192928000000000 intervals after UUID epoch
UUIDEpochOffset = 122192928000000000,

% Your past datetime, converted to UUIDv1 timestamp (100ns intervals since 1582-10-15)
UnixTimestamp = Time, % Example: 2023-01-01 12:00:00 in 100ns intervals since Unix epoch
Timestamp = UnixTimestamp + UUIDEpochOffset,

% Split the timestamp into UUIDv1 fields
TimeLow = Timestamp band 16#FFFFFFFF,
TimeMid = (Timestamp bsr 32) band 16#FFFF,
TimeHigh = (Timestamp bsr 48) band 16#0FFF,

% Version 1 UUID (0001)
TimeHighAndVersion = (TimeHigh bor (1 bsl 12)) band 16#FFFF,

% Generate 14-bit clock sequence with variant bits ('10' in the first two most significant bits)
ClockSeq = (rand:uniform(16#3FFF) band 16#3FFF) bor 16#8000,

% Fixed 48-bit node ID (mock MAC address or random bytes)
%NodeId = <<106,218,190,184,140,5>>, % Exactly 6 bytes (48 bits)

% Assemble the UUIDv1 according to RFC 4122 bit layout
UUID = <<
    TimeLow:32,                          % time_low (4 bytes)
    TimeMid:16,                           % time_mid (2 bytes)
    TimeHighAndVersion:16,                % time_high_and_version (2 bytes)
    (ClockSeq bsr 8):8,                   % clock_seq_hi_and_reserved (upper 8 bits)
    (ClockSeq band 16#FF):8,              % clock_seq_low (lower 8 bits)
    NodeId/binary                         % node (6 bytes)
>>,

% Convert UUID binary to string format
 kass_utils:to(binary, uuid:uuid_to_string(UUID)).