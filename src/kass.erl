-module(kass).
%-include("kass.hrl").

-export([put/2
         ,get/1
         % ,get/1
         % ,update/2
         % ,delete/2]
        ]).


put(TableName, #{uuid := Uuid} = Data) ->
  Query = kass_utils:put_query(TableName, Data),
  case kass_cassandra:query(Query) of
    {ok, void} -> #{uuid => Uuid};
    Error -> Error
  end;
put(TableName, Data) when is_map(Data) ->
  Query = kass_utils:put_query(TableName, Data),
  kass_cassandra:query(Query).

get(TableName) ->
  TableNameStr= kass_utils:to(list, TableName),
  Query = "SELECT * FROM " ++ TableNameStr ++ ";",
  kass_utils:kass_get(kass_cassandra:query(Query)).

% get(TableName, Key) ->
%     Query = "SELECT * FROM " ++ TableName ++ " WHERE key = '" ++ Key ++ "';",
%     kass_cassandra:query(Query).
