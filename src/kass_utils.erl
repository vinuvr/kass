-module(kass_utils).

% -define(KEY_TO_ATOM, fun
%                        (Key, Val, Acc)  when is_map(Val) ->
%                          Acc#{to_atom(Key) => Val}
%                      end).

-export([to/2,
         proplist_to_map/1,
         put_query/2,
         update_query/2,
         cassandra_filter_query/2,
         uri_encode/1,
         read_body/1,
         json_to_map/1,
         tmap/1,
         map_to_json/1,
         map_to_record/3,
         record_to_map/2,
         date_to_binary/1,
         uuid/0,
         uuid_bin/0,
         uuid_str/0,
         uuid_bin/1,
         key_tmap/1,
         uuid_str/1,
         is_uuid/1,
         get_val/2,
         get_val/3,
         get_env/1,
         md5/1,
         parse_file/1,
         current_tstamp/0,
         day_of_the_week/1,
         find_map/3,
         delete_map/3,
         replace_map/4,
         datetime_to_nano_seconds/1,
         nano_seconds_to_datetime/1,
         join_binaries/2,
         send_http_request/2,
         generate_password/0,
         generate_password/1,
         format/2,
         update_map/1,
         create_tid/0,
         urldecode/1,
         urlencode/1,
         parse_query_string/1,
         urlsplit/1,
         urlunsplit/1,
         search_query/1,
         frame_metadata/1,
         frame_meta_params/1,
         frame_meta_query_params/1,
         curate_get_options/1,
         query_to_map/3,
         merge_maps/1,
         try_atomify_keys/1,
         reply_headers/1,
         required_params/1,
         get_response_body/1,
         hrm_api/3,
         kass_get/1,
         filtered_map/1,
         remove_non_ascii/1,
         conversation_id/2,
         get_query/1,
         year_month/1        ]).


conversation_id(To, From) ->
  S = lists:sort([To, From]),
  Hash = crypto:hash(sha256, io_lib:format("~p-~p", S)),
  <<A:32, B:16, C:16, D:16, E:48>> = binary:part(Hash, 0, 16),
  C1 = (C band 16#0FFF) bor (5 bsl 12),   % Apply version 5
  D1 = (D band 16#3FFF) bor (2 bsl 14),   % Apply variant
  UUID = <<A:32, B:16, C1:16, D1:16, E:48>>,
  list_to_binary(uuid:uuid_to_string(UUID)).

year_month(Actime) ->
  %{{Y, M, _}, _} = calendar:gregorian_days_to_date(Actime),
  {{Y, M,_ }, _} = calendar:system_time_to_universal_time(Actime, millisecond),
  case M < 10 of
    true -> kass_utils:to(list, Y) ++ "0" ++ kass_utils:to(list, M);
    _ -> kass_utils:to(list, Y) ++ kass_utils:to(list, M)
  end.

uri_encode(Name) ->
  uri_string:quote(Name).

to(_Type, undefined) ->
  undefined;
to(atom, Val) ->
  to_atom(Val);
to(list, Val) ->
  to_list(Val);
to(binary, Val) ->
  to_binary(Val);
to(integer, Val) ->
  to_integer(Val);
to(map, Val) ->
  to_map(Val);
to(pid, Val) ->
  to_pid(Val);
to(_, Val) ->
  Val.

to_pid(Val) when is_pid(Val) ->
  Val;
to_pid(Val) when is_list(Val) ->
  list_to_pid(Val);
to_pid(Val) when is_binary(Val) ->
  list_to_pid(binary_to_list(Val)).

to_map(Val) when is_map(Val) ->
  Val;
to_map(Val) when is_list(Val) ->
  maps:from_list(Val);
to_map(Val) when is_tuple(Val) ->
  maps:from_list(tuple_to_list(Val)).

to_atom(Val) when is_atom(Val) ->
  Val;
to_atom(Val) when is_list(Val) ->
  list_to_atom(Val);
to_atom(Val) when is_binary(Val) ->
  erlang:binary_to_atom(Val, utf8).

to_list(Val) when is_list(Val) ->
  Val;
to_list(Val) when is_atom(Val) ->
  atom_to_list(Val);
to_list(Val) when is_binary(Val) ->
  binary_to_list(Val);
to_list(Val) when is_pid(Val) ->
  list_to_binary(pid_to_list(Val));
to_list(Val) when is_integer(Val) ->
  integer_to_list(Val);
to_list(Val) when is_map(Val) ->
  maps:to_list(Val).

key_tmap(Map) ->
  maps:fold(fun(K, V, Ac) ->
                Ac#{kass_utils:to(atom, K) => V} end
            ,#{}, Map).
to_binary(Val) when is_binary(Val) ->
  Val;
to_binary([H|_]=Val) when is_tuple(H) ->
  Val;
to_binary(Val) when is_atom(Val) ->
  erlang:atom_to_binary(Val, utf8);
to_binary(Val) when is_list(Val) ->
  list_to_binary(Val);
to_binary(Val) when is_integer(Val) ->
  integer_to_binary(Val);
to_binary(Val) when is_map(Val) ->
  map_to_binary(Val);
to_binary(Val) when is_pid(Val) ->
  list_to_binary(pid_to_list(Val)).

map_to_binary(Map) ->
  maps:fold(fun(Key, Val, Acc) when is_binary(Key),
                                    is_binary(Val)->
                Acc#{Key => Val};
               (Key, Val, Acc) ->
                Acc#{to(binary, Key) => to(binary, Val)}
            end,
            #{},
            Map).

to_integer(Val) when is_integer(Val) ->
  Val;
to_integer(Val) when is_list(Val) ->
  list_to_integer(Val);
to_integer(Val) when is_binary(Val) ->
  binary_to_integer(Val).

%% This is to read complete body from the cowboy request
read_body(Req) ->
  read_body(Req, <<>>).

read_body(Req, Acc) ->
  case cowboy_req:read_body(Req) of
    {ok, Data, Req1} ->
      {ok, << Acc/binary, Data/binary>>, Req1};
    {more, Data, Req1} ->
      read_body(Req1, << Acc/binary, Data/binary >>)
  end.

%% Generate UUID in binary
uuid() ->
  list_to_binary(uuid:uuid_to_string(uuid:get_v4())).

uuid_bin() ->
  to(binary, uuid_str()).

uuid_str() ->
  uuid:uuid_to_string(uuid:get_v4()).

uuid_bin(Uuid) when is_binary(Uuid) ->
  Uuid;
uuid_bin(Uuid) ->
  uuid:uuid_to_string(Uuid).

uuid_str(Uuid) when is_list(Uuid) ->
  Uuid;
uuid_str(Uuid) ->
  uuid:string_to_uuid(Uuid).

is_uuid(Uuid) ->
  case catch uuid:string_to_uuid(to(string, Uuid)) of
    {'EXIT', _} -> false;
    _ -> true
  end.
%% Convert JSON data to Map data using jsx
json_to_map(Json) ->
  jsx:decode(Json, [return_maps]).

%% Convert Json data to Map required by tivan using jsx
tmap(Json) ->
  maps:fold(fun(K, V, Ac) ->
                Ac#{kass_utils:to(atom, K) => V} end
            ,#{}, jsx:decode(Json, [return_maps])).
%% convert Map to JSON
map_to_json(Map) ->
  jsx:encode(Map).

update_map(Map) ->
  Keys = maps:keys(Map),
  lists:foldl(fun(X, ResultMap) ->
                  case maps:get(X, Map) of
                    undefined ->
                      ResultMap#{X => null};
                    _ ->
                      ResultMap
                  end
              end, Map, Keys).

%% convert map to record based on table name and attributes of the table
map_to_record(Table, Attributes, DataMap) ->
  Id = getId(DataMap),
  ValuesList = lists:foldl(
                 fun({id, _, _Default}, Acc) ->
                     [Id|Acc];
                    ({meta, _, Default}, Acc) ->
                     MetaMap = getMeta(DataMap, Default),
                     [MetaMap|Acc];
                    ({Name, Type, Default}, Acc) ->
                     Value = maps:get(to(binary, Name),
                                      DataMap,
                                      Default),
                     ValueMod = to(Type, Value),
                     [ValueMod|Acc];
                    (Name, Acc) ->
                     Value = maps:get(to(binary, Name),
                                      DataMap,
                                      undefined),
                     ValueMod = to(binary, Value),
                     [ValueMod|Acc]
                 end,
                 [],
                 Attributes),
  {Id, list_to_tuple([Table|ValuesList])}.

getId(DataMap) ->
  case catch maps:get(<<"id">>, DataMap) of
    {'EXIT', _} ->
      uuid_bin();
    IdBin ->
      IdBin
  end.

getMeta(DataMap, Default) ->
  case catch maps:get(<<"meta">>, DataMap) of
    {'EXIT', _} ->
      Default#{<<"cdt">> => date_to_binary(erlang:localtime()),
               <<"mdt">> => date_to_binary(erlang:localtime())
              };
    MetaMap ->
      MetaMap#{<<"cdt">> := date_to_binary(erlang:localtime())}
  end.

%% convert erlang localtime to binary
date_to_binary({{Year, Month, Date}, {Hour, Min, Sec}}) ->
  YearString = io_lib:format("~B-~2..0B-~2..0B",[Year, Month, Date]),
  TimeString = io_lib:format("~2..0B:~2..0B:~2..0B",[Hour, Min, Sec]),
  DateString = YearString ++ "T" ++ TimeString,
  to(binary, DateString).

%% Convert Record to map based on list of attributes
record_to_map(DataRecord, Attributes) ->
  [_H|Values] = erlang:tuple_to_list(DataRecord),
  record_to_map(Attributes, Values, #{}).

record_to_map([], _, Map) ->
  Map;
record_to_map([{Name, list, _Default}|NameRest]
              ,[Value|ValRest], Map) when is_list(Value) ->
  NameBin = to(binary, Name),
  record_to_map(NameRest, ValRest,
                Map#{NameBin => Value}
               );
record_to_map([{Name, _Type, _Default}|NameRest], [Value|ValRest], Map) ->
  ValueMod = to(binary, Value),
  NameBin = to(binary, Name),
  record_to_map(NameRest, ValRest,
                Map#{NameBin => ValueMod}
               );
record_to_map([Name|NameRest], [Value|ValRest], Map) ->
  ValueMod = to(binary, Value),
  NameBin = to(binary, Name),
  record_to_map(NameRest, ValRest,
                Map#{NameBin => ValueMod}
               ).

%% Fetch the value from the list, if not present return the default value%
get_val(Key, List) ->
  get_val(Key, List, undefined).

get_val(Key, List, Default) ->
  case lists:keyfind(Key, 1, List) of
    false ->
      Default;
    {_, Value} ->
      Value
  end.

%% get_env -> Get the application environment value
get_env(Key) ->
  application:get_env(Key).

% Generate md5 in hex for a string passed to it
md5(String) ->
  Binary = erlang:md5(String),
  %Integer = crypto:bytes_to_integer(Binary),
  %httpd_util:integer_to_hexlist(Integer).
  Hex = << << if N >= 10 -> N -10 + $a;
                 true -> N + $0 end >> || <<N:4>> <= Binary >>,
  kass_utils:to(list, Hex).

%% This will parse file and get the record definitions from that file
%% The record definitions will be of type
%% {Recordname, RecordAttributesList}
%% {Recordname, [{AttrName, AttrType, AttrDefaultVal}|...]}
parse_file(Module) ->
  {_, _Binary, Path} = code:get_object_code(Module),
  {ok, {_, [
            {abstract_code,
             {_, Attributes}
            }
           ]
       }} = beam_lib:chunks(Path, [abstract_code]),
  RecordsList = lists:foldl(fun({attribute, _, record
                                 ,{RecordName, RecordFields}}, Acc) ->
                                RecordFieldsU =
                                parse_record_fields(RecordFields),
                                [{RecordName, RecordFieldsU}|Acc];
                               (_, Acc) ->
                                Acc
                            end,
                            [],
                            Attributes
                           ),
  maps:from_list(RecordsList).

parse_record_fields(RecordFields) ->
  lists:foldl(fun(RecordForm, Acc) ->
                  case parse_record_form(RecordForm) of
                    unknown ->
                      Acc;
                    Val ->
                      [Val|Acc]
                  end
              end,
              [],
              RecordFields
             ).

parse_record_form({typed_record_field,
                   {record_field, _, {_, _, Name}} = _RecordField,
                   {_, _, Type, _} = _TypeField
                  }) ->
  {Name, Type, undefined};
parse_record_form({typed_record_field,
                   {record_field, _, {_, _, Name}, Default},
                   {_, _, Type, _}
                  }) ->
  DefaultNormalise = erl_parse:normalise(Default),
  {Name, Type, DefaultNormalise};
parse_record_form({record_field, _, {_, _, Name}}) ->
  {Name, binary, undefined};
parse_record_form({record_field, _, {_, _, Name}, _}) ->
  {Name, binary, undefined};
parse_record_form(_Unknown) ->
  unknown.

%% current_tstamp -> Returns current timestamp in gregorian seconds
current_tstamp() ->
  calendar:datetime_to_gregorian_seconds(calendar:universal_time()).

day_of_the_week({_Year, _Month, _Day} = Date) ->
  day_of_the_week(calendar:day_of_the_week(Date));
day_of_the_week(1) -> <<"Monday">>;
day_of_the_week(2) -> <<"Tuesday">>;
day_of_the_week(3) -> <<"Wednesday">>;
day_of_the_week(4) -> <<"Thursday">>;
day_of_the_week(5) -> <<"Friday">>;
day_of_the_week(6) -> <<"Saturday">>;
day_of_the_week(7) -> <<"Sunday">>.

find_map(_Key, _Value, []) -> false;
find_map(Key, Value, [Map|RestMaps]) ->
  case maps:find(Key, Map) of
    {ok, Value} ->
      Map;
    _ ->
      find_map(Key, Value, RestMaps)
  end.

delete_map(_Key, _Value, []) -> [];
delete_map(Key, Value, [Map|RestMaps]) ->
  case maps:find(Key, Map) of
    {ok, Value} ->
      RestMaps;
    _ ->
      [Map|delete_map(Key, Value, RestMaps)]
  end.

replace_map(_Key, _Value, Map, []) -> [Map];
replace_map(Key, Value, Map, [OldMap|RestMaps]) ->
  case maps:find(Key, OldMap) of
    {ok, Value} ->
      [Map|RestMaps];
    _ ->
      [OldMap|replace_map(Key, Value, Map, RestMaps)]
  end.

datetime_to_nano_seconds({{_, _, _}, {_, _, _}} = DateTime) ->
  Seconds = datetime_to_epoch(DateTime),
  Seconds * 1000000000.

datetime_to_epoch(DateTime) ->
  UniversalDateTime = erlang:localtime_to_universaltime(DateTime),
  gregorian_seconds_to_epoch(calendar:datetime_to_gregorian_seconds
                             (UniversalDateTime)).

gregorian_seconds_to_epoch(Secs) ->
  EpochSecs = epoch_gregorian_seconds(),
  Secs - EpochSecs.

epoch_gregorian_seconds() ->
  calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}}).

nano_seconds_to_datetime(NanoSec) ->
  MegaSec = NanoSec div 1000000000000000,
  Sec = (NanoSec rem 1000000000000000) div 1000000000,
  MillSec = ((NanoSec rem 1000000000000000) rem 1000000000),
  MicroSec = MillSec div 1000,
  Now = {MegaSec,Sec,MicroSec},
  calendar:now_to_local_time(Now).

join_binaries(ListOfBinaries, Sep) ->
  lists:foldl(fun(Binary, Acc) ->
                  join_binaries(Binary, Acc, Sep)
              end,
              <<>>,
              ListOfBinaries).

join_binaries(Arg, <<>>, _Sep) -> Arg;
join_binaries(Arg, Acc, Sep) -> <<Acc/binary, Sep/binary, Arg/binary>>.

send_http_request(ReqMethod, ReqParams) ->
  Url  = maps:get(url, ReqParams),
  Header = maps:get(header, ReqParams, []),
  Type = maps:get(type,ReqParams, []),
  Body = maps:get(body, ReqParams, []),
  Request = case ReqMethod of
              get ->
                {binary_to_list(Url), Header};
              _ ->
                {binary_to_list(Url), Header, Type, Body}
            end,
  lager:info("The request is ~p, ~p",[ReqMethod, Request]),
  case httpc:request(ReqMethod, Request %{Url, Header, Type, Body}
                     ,[{timeout, timer:seconds(10)}], []) of
    {ok, {{_Ver, 200, _Reason}, _Headers, Out}}->
      DecodeMsg = get_response_body(Out),
      lager:info("~nDecoded message  == ~p~n",[DecodeMsg]),
      {ok, DecodeMsg};
    {ok, {{_, 201, _}, _, Out}} ->
      {ok, Out};
    {ok, {{_, 204, _}, _, Out}} ->
      {ok, Out};
    {ok, {{_, 409, _}, _, _}} ->
      {ok, []};
    {ok, {{_Ver ,400, _Reason}, _Headers, Out}} ->
      DecodeMsg = get_response_body(Out), %jsx:decode(Out, [return_maps]),
      lager:info("~nDecoded message  == ~p~n",[DecodeMsg]),
      {400, error, failed};
    {ok, {{_Ver ,401, _Reason}, _Headers, Out}} ->
      DecodeMsg = get_response_body(Out), %jsx:decode(Out, [return_maps]),
      lager:info("~nDecoded message  == ~p~n",[DecodeMsg]),
      {401, error, unauthorized};
    {ok,{{_Ver, 402, _Reason}, _Headers, Out}} ->
      DecodeMsg = get_response_body(Out), %jsx:decode(Out, [return_maps]),
      lager:info("~nDecoded message  == ~p~n",[DecodeMsg]),
      {402, error, error};
    {ok, {{_Ver, 404, _Reason}, _Headers, Out}} ->
      DecodeMsg = get_response_body(Out), %jsx:decode(Out, [return_maps]),
      lager:info("~nDecoded message  == ~p~n",[DecodeMsg]),
      {404, error, not_found};
    Error ->
      lager:info("Error occured:: ~p", [Error]),
      {400, error, error}
  end.

get_response_body(Out) when is_binary(Out) ->
  jsx:decode(Out, [return_maps]);
%   % {'EXIT', Reason} ->
%   %   lager:error("decode failed ~p",[Reason]),
%   %   lists:member("SUCCESS", string:tokens(Out, "|"))
%   %   Decode -> Decode
%  %end;
get_response_body(Out) ->
  try
    OutU = kass_utils:to(binary, Out),
    jsx:decode(OutU, [return_maps])
  catch
    _:_ ->
      kass_utils:to(binary, Out)
  end.
% case lists:member("SUCCESS", string:tokens(Out, "|")) of
%   true -> #{status => <<"success">>};
%   _ -> #{status => <<"failed">>
%  ,reason =>
% kass_utils:to(binary, Out).
% }
% end.


generate_password() ->
  generate_password(8).

generate_password(L) when is_integer(L) ->
  Alphabets = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"
               ,"k", "l", "m", "n", "o", "p", "q", "r", "s", "t"
               ,"u", "v", "w", "x", "y", "z"],
  Integers = ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"],
  SpecialChara = ["!", "@", "#", "$", "%", "^", "&", "*", "(", ")", "-"],
  list_to_binary(
    lists:foldl(
      fun(1, Ac) ->  R = rand:uniform(26),
                     if R > 0  ->
                          FL = lists:nth(R, Alphabets),
                          string:uppercase(FL) ++ Ac;
                        true ->
                          "G" ++ Ac
                     end;
         (2, Ac) ->  R = rand:uniform(10),
                     if R > 0 ->
                          FI = lists:nth(R, Integers),
                          FI ++ Ac;
                        true ->
                          "0" ++ Ac
                     end;
         (3, Ac) ->  R = rand:uniform(11),
                     if R > 0 ->
                          SC = lists:nth(R, SpecialChara),
                          SC ++ Ac;
                        true ->
                          "&" ++ Ac
                     end;
         (_, Ac) ->  R = rand:uniform(26),
                     if R > 0 ->
                          FI = lists:nth(R, Alphabets),
                          FI ++ Ac;
                        true ->
                          "e" ++ Ac
                     end
      end, "", lists:seq(1, L)
     )
   );
generate_password(_) ->
  invalid_argument_type.

format(Format, ParamList) ->
  lists:flatten(io_lib:format(Format, ParamList)).

create_tid() ->
  integer_to_binary(erlang:system_time(), 36).

urldecode(Source) ->
  urldecode(Source, <<>>).

-spec urldecode(binary(), binary()) -> binary().
urldecode(<<>>, Acc) ->
  Acc;

urldecode(<<$+, R/binary>>, Acc) ->
  urldecode(R, <<Acc/binary, " ">>);

urldecode(<<$%, H, L, R/binary>>, Acc) ->
  Code  = <<H, L>>,
  Ascii = list_to_integer(binary_to_list(Code), 16),

  urldecode(R, <<Acc/binary, Ascii>>);

urldecode(<<H, R/binary>>, Acc) ->
  urldecode(R, <<Acc/binary, H>>).

urlencode(Source) when is_binary(Source) ->
  urlencode(Source, <<>>);

urlencode(Source) when is_atom(Source) ->
  urlencode(list_to_binary(atom_to_list(Source)), <<>>);

urlencode(Source) when is_list(Source) ->
  urlencode(list_to_binary(Source), <<>>);

urlencode(Source) when is_integer(Source) ->
  urlencode(list_to_binary(integer_to_list(Source)), <<>>);

%% @todo fix this when we move to > R15
urlencode(Source) when is_float(Source) ->
  List = float_to_list(Source),
  Proper = string:substr(List, 1, string:chr(List, $.)+2),
  urlencode(list_to_binary(Proper), <<>>).

urlencode(<<>>, Acc) ->
  Acc;
urlencode(<<$\s, R/binary>>, Acc) ->
  urlencode(R, <<Acc/binary, $+>>);
urlencode(<<C, R/binary>>, Acc) ->
  case C of
    $. -> urlencode(R, <<Acc/binary, C>>);
    $- -> urlencode(R, <<Acc/binary, C>>);
    $~ -> urlencode(R, <<Acc/binary, C>>);
    $_ -> urlencode(R, <<Acc/binary, C>>);
    C when C >= $0
           andalso C=< $9 ->
      urlencode(R, <<Acc/binary, C>>);
    C when C >= $a
           andalso C=< $z ->
      urlencode(R, <<Acc/binary, C>>);
    C when C >= $A
           andalso C=< $Z ->
      urlencode(R, <<Acc/binary, C>>);
    _NotSafe ->
      SafeChar = encode_char(C),
      urlencode(R, <<Acc/binary, "%", SafeChar/binary>>)
  end.

encode_char(Char) ->
  case integer_to_list(Char, 16) of
    Val when length(Val) < 2 -> list_to_binary(["0", Val]);
    ProperLen                -> list_to_binary(ProperLen)
  end.

curate_get_options(Options) ->
  curate_get_options(Options, undefined).

curate_get_options(Options, DefaultSortColumn)
  when is_map(Options), is_atom(DefaultSortColumn) ->
  lager:info("curating get options ~p with default sort column ~p"
             ,[Options, DefaultSortColumn]),
  Options1 = curate_sort_column(Options, DefaultSortColumn),
  Options2 = curate_sort_order(Options1),
  curate_cache(Options2).

curate_sort_column(Options, DefaultSortColumn) ->
  case maps:take(sort_column, Options) of
    error ->
      case maps:take('_sort_column', Options) of
        error when DefaultSortColumn /= undefined ->
          Options#{'_sort_column' => DefaultSortColumn};
        error -> Options;
        {value, Column} when is_atom(Column) ->
          Options#{'_sort_column' => Column};
        {value, ColumnBin} ->
          curate_sort_column2(Options, ColumnBin, DefaultSortColumn)
      end;
    {value, Column} when is_atom(Column) ->
      Options#{'_sort_column' => Column};
    {value, ColumnBin} ->
      curate_sort_column2(Options, ColumnBin, DefaultSortColumn)
  end.

curate_sort_column2(Options, ColumnBin, DefaultSortColumn) ->
  case catch binary_to_existing_atom(ColumnBin) of
    {'EXIT', _} when DefaultSortColumn /= undefined ->
      lager:error("Unable to convert ~p to atom ~p", [ColumnBin]),
      Options#{'_sort_column' => DefaultSortColumn};
    {'EXIT', _} ->
      lager:error("Unable to convert ~p to atom ~p", [ColumnBin]),
      Options;
    ColumnAtom -> Options#{'_sort_column' => ColumnAtom}
  end.

curate_sort_order(Options) ->
  case maps:take(sort_order, Options) of
    error ->
      case maps:take('_sort_order', Options) of
        error ->
          Options#{'_sort_order' => asc};
        {value, <<"desc">>} ->
          Options#{'_sort_order' => desc};
        {value, desc} ->
          Options#{'_sort_order' => desc};
        _ ->
          Options#{'_sort_order' => asc}
      end;
    {value, <<"desc">>} ->
      Options#{'_sort_order' => desc};
    {value, desc} ->
      Options#{'_sort_order' => desc};
    _ ->
      Options#{'_sort_order' => asc}
  end.

curate_cache(Options) ->
  case maps:take(cache, Options) of
    error ->
      case maps:take('_cache', Options) of
        error ->
          Options;
        {value, CacheBin} when is_binary(CacheBin) ->
          case catch list_to_ref(binary_to_list(CacheBin)) of
            {'EXIT', _} -> Options;
            Cache -> Options#{'_cache' => Cache}
          end;
        {value, Cache} when is_reference(Cache) ->
          Options#{'_cache' => Cache}
      end;
    {value, CacheBin} when is_binary(CacheBin) ->
      case catch list_to_ref(binary_to_list(CacheBin)) of
        {'EXIT', _} -> Options;
        Cache -> Options#{'_cache' => Cache}
      end;
    {value, Cache} when is_reference(Cache) ->
      Options#{'_cache' => Cache}
  end.

parse_query_string(Source) ->
  parse_query_string('key', Source, <<>>, <<>>, []).

-spec parse_query_string(atom(), binary(), binary(), binary()
                         , list()) -> list().
parse_query_string(_State, <<>>, <<>>, _ValAcc, RetAcc) ->
  RetAcc;

parse_query_string(_State, <<>>, KeyAcc, ValAcc, RetAcc) ->
  Key = urldecode(KeyAcc),
  Val = urldecode(ValAcc),

  RetAcc ++ [{Key, Val}];

parse_query_string(_State, <<$?, R/binary>>, _KeyAcc, _ValAcc, _RetAcc) ->
  parse_query_string('key', R, <<>>, <<>>, []);

parse_query_string('key', <<$=, R/binary>>, KeyAcc, _ValAcc, RetAcc) ->
  parse_query_string('val', R, KeyAcc, <<>>, RetAcc);

parse_query_string('key', <<$;, R/binary>>, KeyAcc, _ValAcc, RetAcc) ->
  Key = urldecode(KeyAcc),
  parse_query_string('key', R, <<>>, <<>>, RetAcc ++ [{Key, <<>>}]);

parse_query_string('key', <<$&, R/binary>>, KeyAcc, _ValAcc, RetAcc) ->
  Key = urldecode(KeyAcc),
  parse_query_string('key', R, <<>>, <<>>, RetAcc ++ [{Key, <<>>}]);

parse_query_string('val', <<$;, R/binary>>, KeyAcc, ValAcc, RetAcc) ->
  Key = urldecode(KeyAcc),
  Val = urldecode(ValAcc),
  parse_query_string('key', R, <<>>, <<>>, RetAcc ++ [{Key, Val}]);

parse_query_string('val', <<$&, R/binary>>, KeyAcc, ValAcc, RetAcc) ->
  Key = urldecode(KeyAcc),
  Val = urldecode(ValAcc),
  parse_query_string('key', R, <<>>, <<>>, RetAcc ++ [{Key, Val}]);

parse_query_string('key', <<C, R/binary>>, KeyAcc, ValAcc, RetAcc) ->
  parse_query_string('key', R, <<KeyAcc/binary, C>>, ValAcc, RetAcc);

parse_query_string('val', <<C, R/binary>>, KeyAcc, ValAcc, RetAcc) ->
  parse_query_string('val', R, KeyAcc, <<ValAcc/binary, C>>, RetAcc).

urlsplit(Source) ->
  {Scheme, Url1}      = urlsplit_s(Source),
  {Location, Url2}    = urlsplit_l(Url1),
  {Path, Query, Frag} = urlsplit_p(Url2, <<>>),
  {Scheme, Location, Path, Query, Frag}.

urlsplit_s(Source) ->
  case urlsplit_s(Source, <<>>) of
    'no_scheme' -> {<<>>, Source};
    ValidScheme -> ValidScheme
  end.

urlsplit_s(<<>>, _Acc) ->
  'no_scheme';

urlsplit_s(<<C, R/binary>>, Acc) ->
  case C of
    $: -> {Acc, R};
    $+ -> urlsplit_s(R, <<Acc/binary, C>>);
    $- -> urlsplit_s(R, <<Acc/binary, C>>);
    $. -> urlsplit_s(R, <<Acc/binary, C>>);
    C when C >= $a
           andalso C =< $z ->
      urlsplit_s(R, <<Acc/binary, C>>);
    C when C >= $A
           andalso C =< $Z ->
      urlsplit_s(R, <<Acc/binary, C>>);
    C when C >= $0
           andalso C =< $9 ->
      urlsplit_s(R, <<Acc/binary, C>>);
    _NoScheme -> 'no_scheme'
  end.

urlsplit_l(<<"//", R/binary>>) ->
  urlsplit_l(R, <<>>);
urlsplit_l(Source) ->
  {<<>>, Source}.

-spec urlsplit_l(binary(), binary()) -> {binary(), binary()}.
urlsplit_l(<<>>, Acc) ->
  {Acc, <<>>};

urlsplit_l(<<$/, _I/binary>> = R, Acc) ->
  {Acc, R};

urlsplit_l(<<$?, _I/binary>> = R, Acc) ->
  {Acc, R};

urlsplit_l(<<$#, _I/binary>> = R, Acc) ->
  {Acc, R};

urlsplit_l(<<C, R/binary>>, Acc) ->
  urlsplit_l(R, <<Acc/binary, C>>).

urlsplit_p(<<>>, Acc) ->
  {Acc, <<>>, <<>>};

urlsplit_p(<<$?, R/binary>>, Acc) ->
  {Query, Frag} = urlsplit_q(R, <<>>),
  {Acc, Query, Frag};

urlsplit_p(<<$#, R/binary>>, Acc) ->
  {Acc, <<>>, R};

urlsplit_p(<<C, R/binary>>, Acc) ->
  urlsplit_p(R, <<Acc/binary, C>>).

urlsplit_q(<<>>, Acc) ->
  {Acc, <<>>};

urlsplit_q(<<$#, R/binary>>, Acc) ->
  {Acc, R};

urlsplit_q(<<C, R/binary>>, Acc) ->
  urlsplit_q(R, <<Acc/binary, C>>).

urlunsplit({S, N, P, Q, F}) ->
  Us = case S of <<>> -> <<>>; _ -> [S, "://"] end,
  Uq = case Q of <<>> -> <<>>; _ -> [$?, Q] end,
  Uf = case F of <<>> -> <<>>; _ -> [$#, F] end,

  iolist_to_binary([Us, N, P, Uq, Uf]).

search_query(Qs) ->
  proplists:get_value(<<"search">>, uri_string:dissect_query(Qs)).

frame_metadata(#{method := Method, operation_id := OperationId, tag := Tag
                 ,security := true ,summary := Summary ,parameters := Parameters
                 , request_body := true , example := Example}) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => Summary,
      operationId => kass_utils:to(binary, OperationId),
      parameters => Parameters,
      security =>  [#{api_key => []}],
      requestBody => #{content =>
                       #{'application/json' =>
                         #{schema => #{type => array, example => Example}}}
                       ,description => << TagBin/binary," details.">>},
      responses => #{
                     <<"200">> => #{description => <<"Retrives ", TagBin/binary
                                                     ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       ," not found" >>}}}};
frame_metadata(#{method := Method, operation_id := OperationId, tag := Tag
                 ,summary := Summary ,parameters := Parameters
                 , request_body := true , example := Example}) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => Summary,
      operationId => kass_utils:to(binary, OperationId),
      parameters => Parameters,
      requestBody => #{content =>
                       #{'application/json' =>
                         #{schema => #{type => array, example => Example}}}
                       ,description => << TagBin/binary," details.">>},
      responses => #{
                     <<"200">> => #{description => <<"Retrives ", TagBin/binary
                                                     ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       ," not found" >>}}}};
frame_metadata(#{method := Method, operation_id := OperationId, tag := Tag, security := true
                 ,summary := Summary ,request_body := true, example := Example}) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => Summary,
      operationId => kass_utils:to(binary, OperationId),
      security =>  [#{api_key => []}],
      requestBody => #{content =>
                       #{'application/json' =>
                         #{schema => #{type => array, example => Example}}}
                       ,description => << TagBin/binary," details.">>},
      responses => #{<<"200">> =>#{description => <<"Retrives ", TagBin/binary
                                                    ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       , " not found" >>}}}};
frame_metadata(#{method := Method, operation_id := OperationId, tag := Tag
                 ,summary := Summary ,request_body := true, example := Example}) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => Summary,
      operationId => kass_utils:to(binary, OperationId),
      requestBody => #{content =>
                       #{'application/json' =>
                         #{schema => #{type => array, example => Example}}}
                       ,description => << TagBin/binary," details.">>},
      responses => #{<<"200">> =>#{description => <<"Retrives ", TagBin/binary
                                                    ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       , " not found" >>}}}};
frame_metadata(#{method := Method, operation_id := OperationId, tag := Tag, security := true
                 , parameters := Parameters, summary := Summary} = _Data) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => Summary,
      operationId => kass_utils:to(binary, OperationId),
      security =>  [#{api_key => []}],
      parameters => Parameters,
      responses => #{<<"200">> => #{description => <<"Retrives ", TagBin/binary
                                                     ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       ," not found" >>}}}};
frame_metadata(#{method := Method, operation_id := OperationId, tag := Tag
                 ,summary := Summary ,parameters := Parameters}) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => Summary,
      operationId => kass_utils:to(binary, OperationId),
      parameters => Parameters,
      responses => #{
                     <<"200">> => #{description => <<"Retrives ", TagBin/binary
                                                     ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       ," not found" >>}}}};
frame_metadata(#{method := Method, operation_id := OperationId, tag := Tag
                 ,security := true, parameters := Parameters } = Data) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => maps:get(summary, Data, <<"Find ",  TagBin/binary
                                           , " by ID">>),
      security =>  [#{api_key => []}],
      operationId => kass_utils:to(binary, OperationId),
      parameters => Parameters,
      responses => #{<<"200">> => #{description => <<"Retrives ", TagBin/binary
                                                     ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       ," not found" >>}}}};

frame_metadata(#{method := Method, operation_id := OperationId, tag := Tag
                 , parameters := Parameters} = Data) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => maps:get(summary, Data, <<"Find ",  TagBin/binary
                                           , " by ID">>),
      operationId => kass_utils:to(binary, OperationId),
      parameters => Parameters,
      responses => #{<<"200">> => #{description => <<"Retrives ", TagBin/binary
                                                     ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       ," not found" >>}}}};
frame_metadata(#{method := Method, operation_id := OperationId, tag := Tag
                 ,security := true, summary := Summary } = _Data) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => Summary,
      operationId => kass_utils:to(binary, OperationId),
      security =>  [#{api_key => []}],
      responses => #{<<"200">> => #{description => <<"Retrives ", TagBin/binary
                                                     ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       ," not found" >>}}}};
frame_metadata(#{method := Method, operation_id := OperationId, tag := Tag
                 , summary := Summary} = _Data) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      operationId => kass_utils:to(binary, OperationId),
      summary => Summary,
      responses => #{<<"200">> => #{description => <<"Retrives ", TagBin/binary
                                                     ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       ," not found" >>}}}};
frame_metadata(#{tag := Tag, method := Method, operation_id := OperationId
                 , security := true } = Data) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => maps:get(summary, Data, <<"Find ",  TagBin/binary
                                           , " by ID">>),
      security =>  [#{api_key => []}],
      operationId => kass_utils:to(binary, OperationId),
      responses => #{<<"200">> => #{description => <<"Retrives ", TagBin/binary
                                                     ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       ," not found" >>}}}};
frame_metadata(#{tag := Tag, method := Method, operation_id := OperationId} = Data) ->
  TagString = kass_utils:to(list, Tag),
  TagBin = kass_utils:to(binary, Tag),
  #{kass_utils:to(atom, Method) =>
    #{tags => [TagString],
      description => << "Retrives ",  TagBin/binary , " information" >>,
      summary => maps:get(summary, Data, <<"Find ",  TagBin/binary
                                           , " by ID">>),
      operationId => kass_utils:to(binary, OperationId),
      responses => #{<<"200">> => #{description => <<"Retrives ", TagBin/binary
                                                     ," description 200 OK">>}
                     ,<<"400">> => #{description => <<"Invalid Id supplied">>}
                     ,<<"404">> => #{description => << TagBin/binary
                                                       ," not found" >>}}}}.

frame_meta_params(Params) ->
  lists:foldl(
    fun(Name, Ac) ->
        NameU = kass_utils:to(binary, Name),
        Map = #{name => << NameU/binary, "Id" >>,
                description => <<"Id of the ", NameU/binary >>,
                in => <<"path">>,
                required => true,
                schema => #{type => string}},
        [Map|Ac]
    end, [], Params).

frame_meta_query_params(Query_Params) ->
  lists:foldl(
    fun(Name, Ac) ->
        NameU = kass_utils:to(binary, Name),
        Map = #{name => << NameU/binary >>,
                description => << "Query Parameter : ",NameU/binary >>,
                in => <<"query">>,
                schema => #{type => string}},
        lager:info("Map : ~p", [Map]),
        [Map|Ac]
    end, [], Query_Params).

merge_maps(Maps) ->
  lists:foldl(fun(M, Ac) -> maps:merge(Ac, M) end, #{} , Maps).

reply_headers(Headers) ->
  lager:info("Headers for all ~p", [Headers]),
  Origin = maps:get(<<"origin">>, Headers, <<"*">>),
  CorsHeaders = maps:get(<<"access-control-request-headers">>
                         , Headers, <<"*">>),
  CorsMethod = maps:get(<<"access-control-request-method">>
                        , Headers, <<"POST">>),
  #{<<"content-type">> => <<"application/json">>
    ,<<"Access-Control-Allow-Headers">> => CorsHeaders
    ,<<"Access-Control-Allow-Methods">> => <<CorsMethod/binary, ",OPTIONS">>
    ,<<"Access-Control-Max-Age">> => <<"1728000">>
    ,<<"Access-Control-Allow-Origin">> => Origin
    ,<<"Access-Control-Allow-Credentials">> => "true"}.

query_to_map(Qs, Integer, Atom) ->
  lager:info("Qs : ~p", [Qs]),
  Res = uri_string:dissect_query(Qs),
  ResU = proplists:to_map(Res),
  ResF = try_atomify_keys(ResU),
  atomify_map(ResF, Integer, Atom).

atomify_map(Map, Integer, Atom) ->
  maps:fold(fun(K, V, Acc) ->
                case lists:member(K, Integer) of
                  true ->
                    Acc#{K => erlang:binary_to_integer(V)};
                  false ->
                    case lists:member(K, Atom) of
                      true ->
                        Acc#{K => erlang:binary_to_atom(V)};
                      false ->
                        Acc#{K => V}
                    end
                end
            end
            , #{}, Map).

try_atomify_keys(Map) ->
  maps:fold(
    fun(Key, Value, Acc) ->
        KeyMaybeAtom = case catch binary_to_existing_atom(Key, latin1) of
                         {'EXIT', _} -> Key;
                         KeyAtom -> KeyAtom
                       end,
        Acc#{KeyMaybeAtom => Value}
    end,
    #{},
    Map
   ).

required_params(Req) ->
  lager:info("required_params"),
  % {_,Token} = cowboy_req:parse_header(<<"authorization">>, Req),
  case maps:get(<<"authorization">>,maps:get(headers, Req, #{}),undefined) of
    undefined ->
      error;
    Token ->
      lager:info("token ~p",[Token]),
      case token_syntax(Token) of
        false ->
          undefined;
        TokenU ->
          lager:info("TokenU ~p",[TokenU]),
          case keycloak_jwt:decode(TokenU) of
            #{<<"sub">> := UserId, <<"azp">> := _ClientN,
              <<"email">> := Email} ->
              case kass_api_users:get_users(#{keycloak_id => UserId, email => Email}) of
                [] ->
                  lager:info("Error 404"),
                  undefined;
                [#{uuid := UserUuid, tenant_id := TenantUuid}] ->
                  lager:info("UserUuid : ~p", [UserUuid]),
                  #{user_id => UserUuid, email => Email, tenant_id => TenantUuid, keycloak_id => UserId}
              end;
            #{<<"sub">> := _, <<"azp">> := ClientN,
              <<"client_id">> := ClientId} when ClientId == ClientN ->
              case maps:get(<<"user-uid">>,maps:get(headers, Req, #{}),undefined) of
                undefined ->
                  error;
                UserId ->
                  case kass_api_users:get_users(#{uuid => UserId}) of
                    [] ->
                      lager:info("Error 404"),
                      undefined;
                    [#{uuid := UserUuid, tenant_id := TenantUuid, email := Email}] ->
                      lager:info("UserUuid : ~p", [UserUuid]),
                      #{user_id => UserUuid, email => Email, tenant_id => TenantUuid, keycloak_id => UserId}
                  end
              end;
            _ ->
              lager:info("invalid token"),
              undefined
          end
      end
  end.

% required_params(Req) ->
%   lager:info("required_params"),
%   % {_,Token} = cowboy_req:parse_header(<<"authorization">>, Req),
%   case maps:get(<<"authorization">>,maps:get(headers, Req, #{}),undefined) of
%     undefined ->
%       error;
%     Token ->
%       lager:info("token ~p",[Token]),
%       #{<<"sub">> := UserId, <<"azp">> := ClientN,
%         <<"email">> := Email} = keycloak_jwt:decode(Token),
%       case kass_api_tenants:get_tenant_clients(#{name => ClientN}) of
%         [] ->
%           lager:info("Tenant Not Found"),
%           undefined;
%         [#{tenant := TenantUuid}] ->
%           lager:info("Required Params Sucess"),
%           case kass_api_users:get_users(#{keycloak_id => UserId, email => Email, tenant_id => TenantUuid}) of
%             [] ->
% 							lager:info("Error 404"),
%               undefined;
%             [#{uuid := UserUuid}] ->
% 							lager:info("UserUuid : ~p", [UserUuid]),
%               #{user_id => UserUuid, email => Email, tenant_id => TenantUuid}
%           end
%       end
%   end.

token_syntax(Token) ->
  case binary:split(Token, <<"bearer ">>) of
    [_, RestA] ->
      RestA;
    _ ->
      case binary:split(Token, <<"Bearer ">>) of
        [_, RestB] ->
          RestB;
        _ ->
          false
      end
  end.

hrm_api(AccessToken, ClientName, UrlU) ->
  Url = kass_api_configs:get_config(hrm, <<"https://dev.hrmnest.com/v3/tenant/">>),
  UrlF = erlang:list_to_binary(binary_to_list(Url) ++ binary_to_list(ClientName) ++ UrlU),
  lager:info("Url : ~p", [UrlF]),
  Type = "application/json",
  Body = [],
  Header = [{"Authorization","Bearer " ++ binary_to_list(AccessToken)}],
  NewRequest = #{url => UrlF, header => Header, type => Type, body => Body},
  case kass_utils:send_http_request(get, NewRequest) of
    error ->
      lager:info("Error occured while creating new access token"),
      {ok, []};
    {Code, error, Msg} ->
      lager:error("Error ~p, ~p", [Code, Msg]),
      {ok, []};
    {ok, NewData} ->
      {ok, NewData};
    Other ->
      lager:info("Error occured while creating new access token = ~p",[Other]),
      {ok, []}
  end.

% proplist_to_map(Data) ->
%   lists:foldl(
%     fun(X, Ac) ->
%         case proplists:get_value(id, X) of
%           UUID when is_binary(UUID) ->
%             [(proplists:to_map(X))#{id => to(binary, uuid:uuid_to_string(UUID))}|Ac];
%           _ -> [proplists:to_map(X)|Ac]
%         end
%     end, [], Data).


%% @doc
%% Convert a proplist to a map, but if the key is uuid or ends in _id, convert the value to a uuid string.
proplist_to_map(Data) ->
  lists:foldl(
    fun(X, Ac) ->
        Map = maps:fold(fun(K, V, Acc) ->
                            KBin = to(binary, K),
                            case KBin of
                              <<"uuid">> -> Acc#{uuid => to_binary(uuid:uuid_to_string(V))};
                              KBin when size(KBin)> 3 ->
                                case binary:part(KBin, size(KBin) - 3, 3) of
                                  <<"_id">> -> Acc#{K => to_binary(uuid:uuid_to_string(V))};
                                  _ -> Acc#{K => V}
                                end;
                              _ -> Acc#{K => V}
                            end
                        end, #{}, proplists:to_map(X)),
        [Map|Ac]
    end, [], Data).


%     case proplists:get_value(id, X) of
%       UUID when is_binary(UUID) ->
%         [(proplists:to_map(X))#{id => to(binary, uuid:uuid_to_string(UUID))}|Ac];
%       _ -> [proplists:to_map(X)|Ac]
%     end
% end, [], Data).

% put_query(Table, Map) ->
%   {QS1, QS2} = maps:fold(
%                fun(K, V, {Q1, Q2}) when K == id; K== meeting_id; K== uuid, K == group ->
%                    {lists:append(Q1, kass_utils:to(list, K)) ++ ","
%                     ,lists:append(Q2, kass_utils:to(list, V)) ++ ","};
%                   (K, V, {Q1, Q2}) ->
%                    {lists:append(Q1, kass_utils:to(list, K)) ++ ","
%                     ,lists:append(Q2, "'") ++ kass_utils:to(list, V) ++ "',"}
%                end, {"INSERT INTO tutorialspoint." ++ to(list, Table) ++ "(", " VALUES ("}, Map), %% rmv tutorialspoint hardcode in future
%   lists:flatten([lists:droplast(QS1), ")", lists:droplast(QS2), ");"]).


put_query(Table, Map) ->
  {QS1, QS2} = maps:fold(
                 fun(K, V, {Q1, Q2}) ->
                     K1 = to(binary, K),
                     case K1 of
                       <<"size">> ->  without_quotes(Q1, Q2, K, V);
                       <<"is_", _/binary>> -> without_quotes(Q1, Q2, K, V);
                       <<"uuid">> -> without_quotes(Q1, Q2, K, V);
                       Other ->
                         case binary:part(Other, size(Other) -3 , 3) of
                           <<"_id">> -> without_quotes(Q1, Q2, K, V);
                           _ -> %lager:info("god gogh entered here")
                             with_quotes(Q1, Q2, K, V)
                         end
                     end end , {"INSERT INTO tutorialspoint." ++ to(list, Table) ++ "(", " VALUES ("}, Map),
  %lager:info("qs1 ~p, qs2 ~p", [QS1, QS2]),
  lists:flatten([lists:droplast(QS1), ")", lists:droplast(QS2), ");"]).

without_quotes(Q1, Q2, K, V) ->
  {lists:append(Q1, kass_utils:to(list, K)) ++ ","
   ,lists:append(Q2, kass_utils:to(list, V)) ++ ","}.

with_quotes(Q1, Q2, K, V) ->
  {lists:append(Q1, kass_utils:to(list, K)) ++ ","
   ,lists:append(Q2, "'") ++  kass_utils:to(list, V) ++ "',"}.

get_query(Map) ->
  QS = maps:fold(
         fun(K, V, Ac) ->
             K1 = to(binary, K),
             case K1 of
               <<"size">> ->  Ac ++ " size = " ++ to(list, V) ++ " AND ";
               <<"is_", _/binary>> -> Ac ++ to(list, K) ++ " = " ++ to(list, V) ++ " AND ";
               <<"uuid">> -> Ac ++ " uuid = " ++ to(list, V) ++ " AND ";
               Other ->
                 case binary:part(Other, size(Other) -3 , 3) of
                   <<"_id">> -> Ac ++ to(list, K) ++ " = " ++ to(list, V) ++ " AND ";
                   _ -> Ac ++ to(list, K) ++ " ='" ++ to(list, V) ++ "' AND "

                 end
             end end, "WHERE ", Map),
  lists:sublist(QS, 1, length(QS)-5) ++ ";".

update_query(Table, #{uuid := Uuid} = Map) ->
  lager:info("dddddddddddddddddddddddd ~p",[Map]),
  UuidStr = to(list, Uuid),
  Qs = maps:fold(
         fun(K, _, Ac) when K == uuid; K == id -> Ac;
            (K, V, Ac) ->
             lists:append(Ac, (to(list, K) ++ "='" ++ to(list, V) ++ "',"))
         end, "UPDATE tutorialspoint." ++ to(list, Table) ++ " SET ", Map), %% rmv tutorialspoint hardcode in future
  lists:flatten([lists:droplast(Qs), "WHERE id=", UuidStr, ";"]).

cassandra_filter_query(Table, Map) ->
  QS = maps:fold(
         fun(K, V, Ac) when K == meeting_id; K == id ->
             lists:append(Ac, (to(list, K) ++ "=" ++ to(list, V) ++ " AND "));
            (K, V, Ac) ->
             lists:append(Ac, (to(list, K) ++ "='" ++ to(list, V) ++ "' AND "))
         end, "SELECT * FROM tutorialspoint." ++ to(list, Table) ++ " WHERE ", Map),
  lists:flatten([lists:sublist(QS, 1, length(QS) - 5)],  " ALLOW FILTERING;").

kass_get({ok, Result}) ->
  case Row = cqerl:all_rows(Result) of
    empty_dataset -> [];
    _ ->
      proplist_to_map(Row)
  end;
kass_get(Other) ->
  lager:info("Got unknown resp ~p",[Other]),
  error.


filtered_map(Map) ->
  maps:fold(
    fun(_, V, Ac) when V == undefined ->
        Ac;
       (K, V, Ac) -> Ac#{K => V} end, #{}, Map).

remove_non_ascii(Binary) when is_binary(Binary) ->
  case unicode:characters_to_list(Binary, utf8) of
    {error, _, _} -> <<>>; % Return empty binary if it's not valid UTF-8
    List ->
      Cleaned = lists:filter(fun(C) ->
                                 (C >= 32 andalso C =< 126)
                                 andalso C /= 39
                                 andalso C /= 34
                             end, List),
      list_to_binary(Cleaned)
       end.




%lists:flatten([lists:droplast(Q1), ")", lists:droplast(Q2), ";").
%  maps:fold(
%    fun(K, V, {Q1, Q2}) ->
%        {lists:append(Q1, kass_utils:to(list, K)) ++ ","
%         ,lists:append(Q2, "'") ++  kass_utils:to(list, V) ++ "' ,"}
%    end, {"INSERT INTO user (", " VALUES ("}
%    ,Map).
%%
%
%  "INSERT INTO user (id, name, password) VALUES (067974b0-1660-400e-b412-70d088b701f1, 'jemson', 'secret');"




