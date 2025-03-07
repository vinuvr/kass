-module(dbmigration).  %remove after use after db migration
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

-export([start_migration/1
         ,add_to_chat_messages_core/1]).

-export([year_month/1]).
%===================================================================
%%% API

start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []). % Adjust args as needed

stop() ->
  gen_server:call(?MODULE, stop).

start_migration(Args) ->
  gen_server:call(?MODULE, {start_migration, Args}).
%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->  % Initialization; replace [] with arguments if necessary
  {ok, #{}}.  % Replace #state{} with your initial state

handle_call({start_migration, Args}, _From, State) ->
  do_start_migration(Args),
  {reply, ok, State};
handle_call(stop, _From, State) ->
  {stop, normal, ok, State}; % Graceful shutdown
handle_call(_Request, _From, State) ->
  Reply = {error, unknown_request},  % Handle other calls here
  {reply, Reply, State}.


handle_cast(_Msg, State) -> % Handle asynchronous messages
  {noreply, State}.

handle_info(_Info, State) -> % Handle timeouts, other messages
  {noreply, State}.

terminate(_Reason, _State) ->  % Cleanup before termination
  ok.

code_change(_OldVsn, State, _Extra) ->  % For hot code swapping
  {ok, State}.

%%%===================================================================
%%% Internal functions and state definition
%%%===================================================================

do_start_migration(Args) ->
  lager:info("start_migration ~p", [Args]),
  lists:foreach(
    fun(#{a_ctime := Actime, attachments := Attachments, body := Body
          ,category := Category, delivered_to := DeliveredTo, edited_time := EditedTime
          ,fcm_notification := FN, forward_from := ForwardedFrom
          ,from := From, group := Group, is_seen := IsSeen
          ,other_info_map := OtherInfoMap, parent_message := Parent_message
          ,pinned := Pinned, pinned_by := Pinnedby
          ,pinned_time := PinnedTime, platform := Platform
          ,reactions := Reactions, reply_to := Replyto
          ,schedule_time := ScheduleTime, seen := Seen
          ,status := Status, tenant := Tenant
          ,to := To, type := Type, uuid := _MsgUUID})->
        %#{<<"plainText">> := Plaintext} = Body,
        Plaintext = get_plaintext(Body),
        Uuid = kass_uuid:time_uuid(Actime*10000),
        CID = conversation_id(To, From, Group),
        YMN = year_month(Actime),
        spawn(fun() ->
                  add_to_chat_messages_core(kass_utils:filtered_map(
                                              #{tenant_id => Tenant
                                                ,conversation_id => CID
                                                ,year_month => YMN
                                                ,uuid => Uuid
                                                ,sender_id => From
                                                ,recipient_id => To
                                                ,type => Type
                                                ,category => Category
                                                ,status => Status
                                                ,is_pinned => Pinned
                                                ,is_group => Group
                                                ,is_seen => IsSeen
                                                ,a_ctime => Actime
                                                ,day => <<"ALL">>
                                                ,body => base64:encode(jsx:encode(Body))
                                                %,body => jsx:encode(Body)
                                                ,plaintext => Plaintext
                                                %,plaintext => base64:encode(Plaintext)
                                                }))
              end),
        spawn(fun() -> add_pinned_messages(kass_utils:filtered_map(
                                             #{conversation_id => CID
                                               ,message_id => Uuid
                                               ,pinned_time => PinnedTime
                                               ,pinned_user_id => Pinnedby
                                               ,is_pinned => Pinned
                                              })) end),
        lists:foreach(
          fun(#{clientid := DUserId, time := DTime}) ->
              spawn(fun() ->
                        DeliveredMap = kass_utils:filtered_map(
                                         #{uuid => kass_utils:uuid_bin(),
                                           message_id => Uuid,
                                           delivered_to_user_id => DUserId,
                                           delivered_time => DTime}),
                        add_delivered_to(DeliveredMap)
                    end);
             (_) -> ok
          end, DeliveredTo),

        lists:foreach(fun(#{timestamp := STime, uuid := SUserId}) ->
                          spawn(fun() ->
                                    SeenMap = kass_utils:filtered_map(
                                                #{uuid => kass_utils:uuid_bin(),
                                                  message_id => Uuid,
                                                  seen_user_id => SUserId,
                                                  seen_time => STime}),
                                    lager:info("got seen by"),
                                    add_seen_by(SeenMap)
                                end);
                         (_) -> ok
                      end, Seen),
        lists:foreach(
          fun(#{emoji := Emoji, member := Members, count := Count}) ->
              spawn(fun()->
                        lists:foreach(
                          fun(Member) ->
                              spawn(fun() ->
                                        ReactionMap = kass_utils:filtered_map(
                                                        #{uuid => kass_utils:uuid_bin(),
                                                          message_id => Uuid,
                                                          user_id => Member,
                                                          emoji => Emoji,
                                                          reaction_time => erlang:system_time(millisecond)
                                                         }),
                                        add_chat_message_user_reactions(ReactionMap)
                                    end)
                              %(Otherss) -> lager:info("got the unkown ~p",[Otherss])
                          end, Members),
                        ReactionCountMap = kass_utils:filtered_map(
                                             #{message_id => Uuid,
                                               emoji => Emoji,
                                               count => Count}),
                        add_chat_message_reaction_counts(ReactionCountMap)
                    end);
             (OOOO) -> lager:info("Got dddddddddddd ~p",[OOOO])
          end, Reactions),
        lists:foreach(
          fun(#{<<"name">> := FName, <<"url">> := FUrl
                ,<<"type">> := FType, <<"size">> := FSize}) ->
              spawn(fun() ->
                        AttachmentMap = kass_utils:filtered_map(
                                          #{uuid => kass_utils:uuid_bin(),
                                            message_id => Uuid,
                                            name => FName,
                                            uploaded_user_id => From,
                                            url => FUrl,
                                            type => FType,
                                            size => FSize}),
                        add_chat_message_attachments(AttachmentMap)
                    end);
             (_Other) -> ok
          end, Attachments),
        spawn(fun() ->
                  OtherInfoMap1 = kass_utils:filtered_map(
                                    #{
                                      message_id => Uuid,
                                      is_fcm_notification => FN,
                                      conversation_id => CID,
                                      schedule_time => ScheduleTime,
                                      platform => Platform,
                                      %edited_time => edited_time(EditedTime, Actime),
                                      reply_to => jsx_encode(Replyto),
                                      forwarded_from => jsx_encode(ForwardedFrom)
                                     }),
                  add_chat_message_additional(OtherInfoMap1)
              end),
        lists:foreach(fun(EpochTime) ->
                          spawn(fun() ->
                                    EpochTimeMap = kass_utils:filtered_map(
                                                     #{uuid => kass_utils:uuid_bin(),
                                                       message_id => Uuid,
                                                       conversation_id => CID,
                                                       edited_time => EpochTime

                                                      }),
                                    add_chat_message_edit_history(EpochTimeMap)
                                end)

                      end, EditedTime),

        spawn(fun() ->
                  ChatConversationMonthMap = kass_utils:filtered_map(
                                               #{tenant_id => Tenant,
                                                 conversation_id => CID,
                                                 year_month => year_month(Actime)
                                                }),
                  add_chat_conversation_month(ChatConversationMonthMap) end

             ),
        lager:info("pppppppppppppppppppppppgoting to create recent chat map"),
        spawn(fun() ->
          RecentChatMap1 = kass_utils:filtered_map(
            
            #{tenant_id => Tenant,
              user_id => From,
              peer_id => To,
              last_message_id => Uuid,
              conversation_id => CID,
              a_ctime => Actime,
              %unread_count => ,
              is_group => Group}),
  
          RecentChatMap2 = kass_utils:filtered_map(
             #{tenant_id => Tenant,
               user_id => To,
               peer_id => From,
               conversation_id => CID,
               last_message_id => Uuid,
               a_ctime => Actime,
               unread_count => 1,
               uuid => Uuid,
               is_group => Group}),

          add_recent_chat(RecentChatMap1),
          add_recent_chat_counter(#{tenant_id => Tenant
                                    ,conversation_id => CID
                                    ,user_id => From}, IsSeen),
          add_recent_chat(RecentChatMap2),
          add_recent_chat_counter(#{tenant_id => Tenant
                                    ,user_id => To
                                    ,conversation_id => CID}, IsSeen)
                                    end
           
          );

       % ReactionMap = kass_utils:filtered_map(
       % #{uuid => kass_utils:uuid_bin(),
       %  message_id => Uuid,
       %  user_id => })
       %   ),
       %   add_chat_message_reaction_counts(ReactionCountMap)

       (Other) -> lager:info("got unknown ~p",[Other])
    end,
    Args).

% edited_time(undefined, Actime) -> Actime;
% edited_time(EditedTime, _Actime) -> EditedTime.

jsx_encode(undefined) -> undefined;
jsx_encode(Data) -> jsx:encode(Data).
%% add_to_kass(Args),
%   spawn(fun() -> add_to_chat_messages_core(Args) end), %add_to_chat_messages_core(Args),
%   add_pinned_messages(Args),
%   add_delivered_to(Args),
%   add_seen_by(Args),
%   add_chat_message_user_reactions(Args),
%   add_chat_message_attachments(Args),
%   add_chat_message_additional(Args),
%   ok.
% time_uuid() ->
%   State = uuid:new(self()),
%   {UUID, _NewState} = uuid:get_v1(State),
%   list_to_binary(uuid:uuid_to_string(UUID)).

get_plaintext(#{<<"plainText">> := PlaintextBin})  -> 
  kass_utils:remove_non_ascii(PlaintextBin);
get_plaintext(_) -> undefined.

year_month(Actime) ->
  %{{Y, M, _}, _} = calendar:gregorian_days_to_date(Actime),
  {{Y, M,_ }, _} = calendar:system_time_to_universal_time(Actime, millisecond),
  case M < 10 of
    true -> kass_utils:to(list, Y) ++ "0" ++ kass_utils:to(list, M);
    _ -> kass_utils:to(list, Y) ++ kass_utils:to(list, M)
  end.
%io_lib:format("~p-~p", [M, Y]).

conversation_id(To, From, false) ->
  S = lists:sort([To, From]),
  Hash = crypto:hash(sha256, io_lib:format("~p-~p", S)),
  <<A:32, B:16, C:16, D:16, E:48>> = binary:part(Hash, 0, 16),
  C1 = (C band 16#0FFF) bor (5 bsl 12),   % Apply version 5
  D1 = (D band 16#3FFF) bor (2 bsl 14),   % Apply variant
  UUID = <<A:32, B:16, C1:16, D1:16, E:48>>,
  list_to_binary(uuid:uuid_to_string(UUID));
conversation_id(To, _, _) ->
  lager:info("it is group"),
  To.

add_to_chat_messages_core(Args) ->
  %ager:info("add_to_chat_messages_core ~p", [Args]),
  Query = kass_utils:put_query(chat_messages_core, Args),
  R = kass_cassandra:query(Query),
  lager:info("add_to_chat_message result ~p and query ~p", [R, Query]).

add_pinned_messages(#{is_pinned := false}) ->
  ok;
add_pinned_messages(Args) ->
  lager:info("p1inned_messages ~p", [Args]),
  Query = kass_utils:put_query(pinned_messages, maps:remove(is_pinned, Args)),
  R = kass_cassandra:query(Query),
  lager:info("pinned_messages result  ~p and query ~p", [R, Query]).

add_delivered_to(Args) ->
  %lager:info("delivered_to ~p", [Args]),
  Query = kass_utils:put_query(delivered_to, Args),
  R = kass_cassandra:query(Query),
  lager:info("delivered_to result  ~p and query ~p", [R, Query]).

add_seen_by(Args) ->
  lager:info("seen_by ~p", [Args]),
  Query = kass_utils:put_query(seen_by, Args),
  R = kass_cassandra:query(Query),
  lager:info("seen_by resutl  ~p and query ~p", [R, Query]).

add_chat_message_user_reactions(Args) ->
  lager:info("chat_message_user_reactions ~p", [Args]),
  Query = kass_utils:put_query(chat_message_user_reactions, Args),
  R = kass_cassandra:query(Query),
  lager:info("chat_message_user_reactions result  ~p and query ~p", [R, Query]).

add_chat_message_reaction_counts(Args) ->
  lager:info("n1ew got chat message reactions count"),
  %Query = kass_utils:put_query(chat_message_reaction_counts, Args),
  Query = reactions_couter_query(Args),
  %lager:info("$$$$$$$$$$$$$$$$$$$$$$$$$$ ~p",[Query]),
  %R = ok,
  R = kass_cassandra:query(Query),
  lager:info("chat_message_reaction_counts result ~p and query ", [R, Query]).

add_chat_message_attachments(Args) ->
  lager:info("chat_message_attachments ~p", [Args]),
  Query = kass_utils:put_query(chat_message_attachments, Args),
  R = kass_cassandra:query(Query),
  lager:info("chat_message_attachments result ~p and query ~p", [R, Query]).

add_chat_message_additional(Args) ->
  lager:info("chat_message_additional ~p", [Args]),
  Query = kass_utils:put_query(chat_message_additional, Args),
  R = kass_cassandra:query(Query),
  lager:info("chat_message_additional result  ~p and query ~p", [R, Query]).

add_chat_message_edit_history(Args) ->
  lager:info("chat_message_epoch_time ~p", [Args]),
  Query = kass_utils:put_query(chat_message_edit_history, Args),
  R = kass_cassandra:query(Query),
  lager:info("chat_message_epoch_time result  ~p and query ~p", [R, Query]).

add_chat_conversation_month(Args) ->
  lager:info("chat_conversation_month ~p", [Args]),
  Query = conversation_month_query(Args),
  R = kass_cassandra:query(Query),
  lager:info("chat_conversation_month result  ~p and query ~p", [R, Query]).

add_recent_chat(Args) ->
  lager:info("add_recent_chat fase ~p", [Args]),
  Query = recent_chat_query(Args, false),
  R = kass_cassandra:query(Query),
  lager:info("add_recent_chat result  ~p and query ~p", [R, Query]).
% add_recent_chat(Args) ->
%   lager:info("add_recent_chat true ~p", [Args]),
%   Query = recent_chat_query(Args, true),
%   R = kass_cassandra:query(Query),
%   lager:info("add_recent_chat result  ~p and query ~p", [R, Query]);
% add_recent_chat(Args) ->
%   lager:info("DDDDDDDDDDDDDDDDDDDDDDDDDDDd ~p",[Args]).

add_recent_chat_counter(Args, Flag) ->
  lager:info("got add recent chat counter ~p",[Args]),
  Query = recent_chat_counter_query(Args, Flag),
  R = kass_cassandra:query(Query),
  lager:info("add recent chat counter resutl ~p and query ~p",[R, Query]).

reactions_couter_query(#{count := Count, message_id := MsgId, emoji := Emoji}) ->
  CountStr = kass_utils:to(list, Count),
  MsgIdStr = kass_utils:to(list, MsgId),
  EmojiStr = kass_utils:to(list, Emoji),
  "UPDATE tutorialspoint.chat_message_reaction_counts SET count = count +"
  ++ CountStr ++ " WHERE message_id = " ++ MsgIdStr ++ " AND emoji = '" ++ EmojiStr
  ++ "';".

conversation_month_query(#{tenant_id := Tenant, conversation_id := CID, year_month := YMN}) ->
  TenantIdStr = kass_utils:to(list, Tenant),
  CIDStr = kass_utils:to(list, CID),
  YMNStr = kass_utils:to(list, YMN),
  "UPDATE tutorialspoint.chat_conversation_months SET message_count = message_count + 1 WHERE tenant_id =" ++
  TenantIdStr ++ " AND conversation_id = " ++ CIDStr ++ " AND year_month = '" ++ YMNStr ++ "';".

recent_chat_query(#{tenant_id := Tenant, conversation_id := CID, user_id := UserId
               ,peer_id := From ,is_group := IsGroup %, uuid := Uuid
               ,last_message_id := Uuid, a_ctime := Actime}, true) ->
  TenantIdStr = kass_utils:to(list, Tenant),
  CIDStr = kass_utils:to(list, CID),
  UserIdStr = kass_utils:to(list, UserId),
  ActimeStr = kass_utils:to(list, Actime),
  UuidStr = kass_utils:to(list, Uuid),
  FromStr = kass_utils:to(list, From),
  GroupStr = kass_utils:to(list, IsGroup),
  "UPDATE tutorialspoint.recent_chats SET last_message_id = "++ UuidStr ++",a_ctime = " ++ ActimeStr ++ 
    ", is_group = " ++ GroupStr ++", peer_id =" ++ FromStr ++  " WHERE tenant_id = " ++ TenantIdStr ++" AND user_id = "
    ++ UserIdStr ++ " AND conversation_id = " ++ CIDStr ++ ";";
  recent_chat_query(#{tenant_id := Tenant, conversation_id := CID, user_id := UserId
               ,peer_id := From , is_group := IsGroup
               ,last_message_id := Uuid, a_ctime := Actime}, false) ->
  TenantIdStr = kass_utils:to(list, Tenant),
  CIDStr = kass_utils:to(list, CID),
  UserIdStr = kass_utils:to(list, UserId),
  ActimeStr = kass_utils:to(list, Actime),
  UuidStr = kass_utils:to(list, Uuid),
  FromStr = kass_utils:to(list, From),
  GroupStr = kass_utils:to(list, IsGroup),
  "UPDATE tutorialspoint.recent_chats SET last_message_id = "++ UuidStr ++",a_ctime = " ++ ActimeStr ++ 
    ",is_group = " ++ GroupStr ++", peer_id =" ++ FromStr ++  " WHERE tenant_id = " ++ TenantIdStr ++" AND user_id = "
    ++ UserIdStr ++ " AND conversation_id = " ++ CIDStr ++ ";".

recent_chat_counter_query(#{tenant_id := Tenant, conversation_id := CID, user_id := UserId}, Flag) ->
  TenantIdStr = kass_utils:to(list, Tenant),
  CIDStr = kass_utils:to(list, CID),
  UserIdStr = kass_utils:to(list, UserId),
  Count = case Flag of
          true -> "0";
_ -> "1" end, 
  "UPDATE tutorialspoint.unread_counts SET unread_count = unread_count + " ++ Count ++
  " WHERE tenant_id = " ++ TenantIdStr ++
  "AND user_id = " ++ UserIdStr ++
  " AND conversation_id = " ++ CIDStr ++ ";".