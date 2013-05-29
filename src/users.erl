-module(users).
-include("user.hrl").
-include("accounts.hrl").
-include("log.hrl").
-include_lib("white_rabbit/include/nsm_mq.hrl").
-compile(export_all).

do_register(#user{username=U} = RegisterData0) ->
  case check_register_data(RegisterData0) of
    ok ->
      HashedPassword = case RegisterData0#user.password of
        undefined -> undefined;
        PlainPassword -> utils:sha(PlainPassword)
      end,
      RegisterData = RegisterData0#user{
        feed     = store:feed_create(),
        direct   = store:feed_create(),
        pinned   = store:feed_create(),
        starred  = store:feed_create(),
        password = HashedPassword},

      store:put(RegisterData),
      %nsx_msg:notify(["system", "put"], RegisterData),
      nsm_accounts:create_account(U),
      % assign quota
      {ok, DefaultQuota} = store:get(config, "accounts/default_quota",  300),
      nsm_accounts:transaction(U, ?CURRENCY_QUOTA, DefaultQuota, #ti_default_assignment{}),
      %% init message queues infrastructure
      init_mq(U, []),
      login_posthook(U),
      {ok, U};
    {error, Error} -> {error, Error}
  end.

register(#user{username=U, email=Email, facebook_id = FbId} = RegisterData0) ->
  FindUser = case check_username(U, FbId) of
    {error, E} -> {error, E};
    {ok, NewName} ->
      case get_user({email, Email}) of
        {error, _NotFound} -> {ok, NewName};
        {ok, _} -> {error, email_taken}
      end
  end,

  FindUser2 = case FindUser of
    {ok, UserName} ->
      case nsm_groups:get_group(UserName) of
        {error, notfound} -> {ok, UserName}; % it means username is free
        _ -> {error, username_taken}
      end;
    SomethingElse -> SomethingElse
  end,

  case FindUser2 of
    {ok, Name} -> do_register(RegisterData0#user{username=Name});
    {error, username_taken} -> {error, user_exist};
    {error, email_taken} ->    {error, email_taken}
  end.

check_username(Name, FbId) ->
  case get_user(Name) of
    {error, notfound} -> {ok, Name};
    {ok, User} when FbId =/= undefined ->
      check_username(User#user.username  ++ integer_to_list(crypto:rand_uniform(0,10)), FbId);
    {ok, _User}-> {error, username_taken}
  end.

% @doc
% Removes user with all tigth connected entries relying on user_id
delete_user(UserName) ->
   % get User record
   case get_user(UserName) of
	{ok, User} ->
	   %% remove from all groups
       GIds = nsm_groups:list_groups_per_user(UserName),
       [nsx_msg:notify(["subscription", "user", UserName, "remove_from_group"], {GId}) || GId <- GIds],
	   %% remove from subcribtions
	   S = list_subscr(User),
	   F2U = [ {MeId, FrId} || #subscription{who = MeId, whom = FrId} <- S ],
	   [ unsubscr_user(MeId, FrId) || {MeId, FrId} <- F2U ],
	   [ unsubscr_user(FrId, MeId) || {MeId, FrId} <- F2U ],
	   store:delete(user_status, UserName),
	   store:delete(user, UserName),
	   {ok, User};
	E -> E
   end.

get_user({username, UserName}) -> store:user_by_username(UserName);
get_user({facebook, FBId}) -> store:user_by_facebook_id(FBId);
get_user({email, Email}) -> store:user_by_email(Email);
get_user(UId) -> store:get(user, UId).

get_all_users() -> store:all(user).

subscribe(Who, Whom) ->
    case is_user_blocked(Who, Whom) of
        false ->
            Record = #subs{who = Who, whom = Whom},
            ok = store:put(Record),
            subscribe_user_mq(user, Who, Whom);
        true -> do_nothing
    end.

unsubscribe(Who, Whom) ->
    case is_user_subscr(Who, Whom) of
        true ->
            ok = store:delete(subs, {Who, Whom}),
            remove_subscription_mq(user, Who, Whom);
        false ->
            do_nothing
    end.

list_subscr(undefined)-> [];
list_subscr(#user{username = UId}) -> list_subscr(UId);
list_subscr(UId) when is_list(UId) -> lists:sort( store:all_by_index(subs, <<"subs_who_bin">>, list_to_binary(UId)) ).
list_subscr(UId, PageNumber, PageAmount) when is_list(UId) -> 
    Offset = case (PageNumber-1)*PageAmount of
        I when is_integer(I), I>0 -> I+1;
        _ -> 1
	 end,
	lists:sublist(list_subscr(UId), Offset, PageAmount).
list_subscr_usernames(UId) -> [UserId || #subs{whom = UserId} <- list_subscr(UId)].

list_subscr_me(#user{username = UId}) -> list_subscr_me(UId);
list_subscr_me(UId) when is_list(UId) -> lists:sort( store:all_by_index(subs, <<"subs_whom_bin">>, list_to_binary(UId)) ).

is_user_subscr(Who, Whom) ->
    case store:get(subs, {Who, Whom}) of
        {ok, _} -> ?INFO("User ~p is a friend of user ~p", [Whom, Who]), true;
        _Response -> ?INFO("User ~p is not a friend of user ~p. Response: ~p", [Whom, Who, _Response]), false
    end.

update_after_login(User) -> %RPC to cleanup
    Update =
        case user_status(User) of
            {error, status_info_not_found} ->
                #user_status{username = User,
                             last_login = erlang:now()};
            {ok, UserStatus} ->
                UserStatus#user_status{last_login = erlang:now()}
        end,
    store:put(Update).

user_status(User) ->
    case store:get(user_status, User) of
        {ok, Status} ->
            {ok, Status};
        {error, notfound} ->
            {error, status_info_not_found}
    end.


user_status(User, Key) ->
    case user_status(User) of
        {ok, Status0} ->
            Fields = record_info(fields, user_status),
            [user_status | List] = tuple_to_list(Status0),
            Status = lists:zip(Fields, List),
            {_, V} = lists:keyfind(Key, 1, Status),
            {ok, V};
        _ ->
            {error, status_info_not_found}
    end.

user_status(User, Key, Value) ->
    case user_status(User) of
        {ok, Status0} ->
            Fields = record_info(fields, user_status),
            [user_status | List] = tuple_to_list(Status0),
            Status = lists:zip(Fields, List),
            NewStatus0 = lists:keyreplace(Key, 1, Status, {Key, Value}),
            NewStatus = [user_status | element(2, lists:unzip(NewStatus0))],
            store:put(list_to_tuple(NewStatus));
        _ ->
            {error, status_info_not_found}
    end.

get_user_by_feed_id(Fid) ->
    store:select(user, fun(#user{feed=F}) when F=:=Fid-> true;(_)->false end).

search_user("") ->
	store:all(user);
search_user(Str) ->
    store:select(user,
        fun(#user{email=E}) when E=:=Str-> true;
        (#user{username=N}) when N=:=Str-> true;
        (#user{name=N})     when N=:=Str-> true;
        (#user{surname=S})  when S=:=Str-> true;
        (#user{surname=S, name=N})      ->
            case S ++ " " ++ N of
                Sum when Sum=:=Str -> true;
                _                  -> false
            end;
        (_)                             ->false
        end).

get_user_game_status(User) ->
    case store:get(user_game_status, User) of
        {ok, #user_game_status{status=Status}} -> Status
        ;_                                     -> "offline"
    end.

set_user_game_status(User, Status) -> store:put(#user_game_status{user=User, status=Status}).

% TODO: game_session:525 move real DB operation from here behind rabbit
%       two level: first message received by session pid
%                   then it goes to change db bg worker

block(Who, Whom) ->
    ?INFO("~w:block_user/2 Who=~p Whom=~p", [?MODULE, Who, Whom]),
    unsubscr_user(Who, Whom),
    store:block_user(Who, Whom),
    nsx_msg:notify_user_block(Who, Whom).

unblock(Who, Whom) ->
    ?INFO("~w:unblock_user/2 Who=~p Whom=~p", [?MODULE, Who, Whom]),
    store:unblock_user(Who, Whom),
    nsx_msg:notify_user_unblock(Who, Whom).

blocked_users(UserId) -> store:list_blocks(UserId).

get_blocked_users_feed_id(UserId) ->
    UsersId = store:list_blocks(UserId),
    Users = store:select(user, fun(#user{username=U})-> lists:member(U, UsersId) end),
    {UsersId, [Fid || #user{feed=Fid} <- Users]}.


is_user_blocked(Who, Whom) -> store:is_user_blocked(Who,Whom).

update_user(#user{username=UId,name=Name,surname=Surname} = NewUser) ->
    OldUser = case store:get(user,UId) of
        {error,notfound} -> NewUser;
        {ok,#user{}=User} -> User
    end,
    store:put(NewUser),
    case Name==OldUser#user.name andalso Surname==OldUser#user.surname of
        true -> ok;
        false -> store:update_user_name(UId,Name,Surname)
    end.

subscribe_user_mq(Type, MeId, ToId) -> process_subscription_mq(Type, add, MeId, ToId).
remove_subscription_mq(Type, MeId, ToId) -> process_subscription_mq(Type, delete, MeId, ToId).
process_subscription_mq(Type, Action, MeId, ToId) ->
    {ok, Channel} = nsm_mq:open([]),
    Routes = case Type of
                 user ->
                     rk_user_feed(ToId);
                 group ->
                     rk_group_feed(ToId)
             end,
    case Action of
        add ->
            bind_user_exchange(Channel, MeId, Routes);
        delete ->
            catch(unbind_user_exchange(Channel, MeId, Routes))
    end,
    nsm_mq_channel:close(Channel),
    ok.

init_mq(User, Groups) ->
    ?INFO("~p init mq. nsm_users: ~p", [User, Groups]),
    UserExchange = ?USER_EXCHANGE(User),
    %% we need fanout exchange to give all information to all users queues
    ExchangeOptions = [{type, <<"fanout">>},
                       durable,
                       {auto_delete, false}],
    {ok, Channel} = nsm_mq:open([]),
    ?INFO("Cration Exchange: ~p,",[{Channel,UserExchange,ExchangeOptions}]),
    ok = nsm_mq_channel:create_exchange(Channel, UserExchange,
                                        ExchangeOptions),
                                          ?INFO("Created OK"),
    %% build routing keys for user's relations
    Relations = build_user_relations(User, Groups),

    %% RK = Routing Key. Bind exchange to all user related keys.
    [bind_user_exchange(Channel, User, RK)
       || RK <- [rk([feed, delete, User])|Relations]],

    nsm_mq_channel:close(Channel),
    ok.

init_mq_for_user(User) ->
    init_mq(User, nsm_groups:list_groups_per_user(User) ).

build_user_relations(User, Groups) ->
    %% Feed Keys. Subscribe for self events, system and groups events
    %% feed.FeedOwnerType.FeedOwnerId.ElementType.ElementId.Action
    %% feed.system.ElementType.Action
    [rk_user_feed(User),
     %% API
     rk( [db, user, User, put] ),
     rk( [subscription, user, User, add_to_group]),
     rk( [subscription, user, User, remove_from_group]),
     rk( [subscription, user, User, leave_group]),
     rk( [login, user, User, update_after_login]),
     rk( [likes, user, User, add_like]),
     rk( [personal_score, user, User, add]),

     rk( [feed, user, User, count_entry_in_statistics] ),
     rk( [feed, user, User, count_comment_in_statistics] ),

     rk( [feed, user, User, post_note] ),

     rk( [subscription, user, User, subscribe_user]),
     rk( [subscription, user, User, remove_subscribe]),
     rk( [subscription, user, User, set_user_game_status]),
     rk( [subscription, user, User, update_user]),
     rk( [subscription, user, User, block_user]),
     rk( [subscription, user, User, unblock_user]),

     rk( [affiliates, user, User, create_affiliate]),
     rk( [affiliates, user, User, delete_affiliate]),
     rk( [affiliates, user, User, enable_to_look_details]),
     rk( [affiliates, user, User, disable_to_look_details]),

     rk( [purchase, user, User, set_purchase_external_id]),
     rk( [purchase, user, User, set_purchase_state]),
     rk( [purchase, user, User, set_purchase_info]),
     rk( [purchase, user, User, add_purchase]),

     rk( [transaction, user, User, add_transaction]),

     rk( [invite, user, User, add_invite_to_issuer]),

     rk( [tournaments, user, User, create]),
     rk( [tournaments, user, User, create_and_join]),

     rk( [gifts, user, User, buy_gift]),
     rk( [gifts, user, User, give_gift]),
     rk( [gifts, user, User, mark_gift_as_deliving]),

     %% system message format: feed.system.ElementType.Action
     rk( [feed, system, '*', '*']) |
     [rk_group_feed(G) || G <- Groups]].

bind_user_exchange(Channel, User, RoutingKey) ->
    {bind, RoutingKey, nsm_mq_channel:bind_exchange(Channel, ?USER_EXCHANGE(User), ?NOTIFICATIONS_EX, RoutingKey)}.

unbind_user_exchange(Channel, User, RoutingKey) ->
    {unbind, RoutingKey, nsm_mq_channel:unbind_exchange(Channel, ?USER_EXCHANGE(User), ?NOTIFICATIONS_EX, RoutingKey)}.

bind_group_exchange(Channel, Group, RoutingKey) ->
    {bind, RoutingKey, nsm_mq_channel:bind_exchange(Channel, ?GROUP_EXCHANGE(Group), ?NOTIFICATIONS_EX, RoutingKey)}.

unbind_group_exchange(Channel, Group, RoutingKey) ->
    {unbind, RoutingKey, nsm_mq_channel:unbind_exchange(Channel, ?GROUP_EXCHANGE(Group), ?NOTIFICATIONS_EX, RoutingKey)}.

init_mq_for_group(Group) ->
    GroupExchange = ?GROUP_EXCHANGE(Group),
    ExchangeOptions = [{type, <<"fanout">>},
                       durable,
                       {auto_delete, false}],   
    {ok, Channel} = nsm_mq:open([]),
    ok = nsm_mq_channel:create_exchange(Channel, GroupExchange, ExchangeOptions),
    Relations = build_group_relations(Group),
    [bind_group_exchange(Channel, Group, RK) || RK <- Relations],
    nsm_mq_channel:close(Channel),
    ok.

build_group_relations(Group) ->
    [
        rk( [db, group, Group, put] ),
        rk( [db, group, Group, update_group] ),
        rk( [db, group, Group, remove_group] ),
        rk( [likes, group, Group, add_like]),   % for comet mostly
        rk( [feed, delete, Group] ),
        rk( [feed, group, Group, '*', '*', '*'] )
    ].


rk(List) ->
    nsm_mq_lib:list_to_key(List).

rk_user_feed(User) ->
    rk([feed, user, User, '*', '*', '*']).

rk_group_feed(Group) ->
    rk([feed, group, Group, '*', '*', '*']).

retrieve_connections(Id,Type) ->
    Friends = case Type of 
                  user -> nsm_users:list_subscr_usernames(Id);
                     _ -> nsm_groups:list_group_members(Id) end,
    case Friends of
	[] -> [];
	Full -> Sub = lists:sublist(Full, 10),
                case Sub of
                     [] -> [];
                      _ -> Data = [begin case store:get(user,Who) of
                                       {ok,User} -> RealName = nsm_users:user_realname_user(User),
                                                    Paid = nsm_accounts:user_paid(Who),
                                                    {Who,Paid,RealName};
				               _ -> undefined end end || Who <- Sub],
			   [X||X<-Data, X/=undefined] end end.
