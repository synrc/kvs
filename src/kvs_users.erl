-module(kvs_users).
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/log.hrl").
-include_lib("kvs/include/feed_state.hrl").
-include_lib("mqs/include/mqs.hrl").
-compile(export_all).

register(#user{username=U, email=Email, facebook_id = FbId} = RegisterData0) ->
    FindUser = case check_username(U, FbId) of
        {error, E} -> {error, E};
        {ok, NewName} -> case kvs_users:get({email, Email}) of
            {error, _} -> {ok, NewName};
            {ok, _} -> {error, email_taken} end end,

    FindUser2 = case FindUser of
        {ok, UserName} -> case groups:get(UserName) of
            {error, _} -> {ok, UserName};
            _ -> {error, username_taken} end;
        A -> A end,

    case FindUser2 of
        {ok, Name} -> process_register(RegisterData0#user{username=Name});
        {error, username_taken} -> {error, user_exist};
        {error, email_taken} ->    {error, email_taken} end.

process_register(#user{username=U} = RegisterData0) ->
    HashedPassword = case RegisterData0#user.password of
        undefined -> undefined;
        PlainPassword -> utils:sha(PlainPassword) end,
    RegisterData = RegisterData0#user {
        feed     = kvs:feed_create(),
        direct   = kvs:feed_create(),
        pinned   = kvs:feed_create(),
        starred  = kvs:feed_create(),
        password = HashedPassword },

    kvs:put(RegisterData),
    accounts:create_account(U),
    {ok, DefaultQuota} = kvs:get(config, "accounts/default_quota",  300),
    accounts:transaction(U, quota, DefaultQuota, #tx_default_assignment{}),
    init_mq(U),
    {ok, U}.

check_username(Name, FbId) ->
    case kvs_users:get(Name) of
        {error, notfound} -> {ok, Name};
        {ok, User} when FbId =/= undefined -> check_username(User#user.username  ++ integer_to_list(crypto:rand_uniform(0,10)), FbId);
        {ok, _}-> {error, username_taken} end.

delete(UserName) ->
    case kvs_users:get(UserName) of
        {ok, User} -> GIds = groups:list_groups_per_user(UserName),
                      [nsx_msg:notify(["subscription", "user", UserName, "remove_from_group"], {GId}) || GId <- GIds],
                      F2U = [ {MeId, FrId} || #subscription{who = MeId, whom = FrId} <- subscriptions(User) ],
                      [ unsubscribe(MeId, FrId) || {MeId, FrId} <- F2U ],
                      [ unsubscribe(FrId, MeId) || {MeId, FrId} <- F2U ],
                      kvs:delete(user_status, UserName),
                      kvs:delete(user, UserName),
                      {ok, User};
                 E -> E end.

get({username, UserName}) -> kvs:user_by_username(UserName);
get({facebook, FBId}) -> kvs:user_by_facebook_id(FBId);
get({email, Email}) -> kvs:user_by_email(Email);
get(UId) -> kvs:get(user, UId).

subscribe(Who, Whom) ->
    Record = #subscription{key={Who,Whom}, who = Who, whom = Whom},
    kvs:put(Record),
    subscription_mq(user, add, Who, Whom).

unsubscribe(Who, Whom) ->
    case subscribed(Who, Whom) of
        true  -> kvs:delete(subscription, {Who, Whom}),
                 subscription_mq(user, remove, Who, Whom);
        false -> skip end.

subscriptions(undefined)-> [];
subscriptions(#user{username = UId}) -> subscriptions(UId);
subscriptions(UId) when is_list(UId) -> lists:sort( kvs:all_by_index(subs, <<"subs_who_bin">>, list_to_binary(UId)) ).

subscribed(Who, Whom) ->
    case kvs:get(subscription, {Who, Whom}) of
        {ok, _} -> true;
        _ -> false end.

update_user(#user{username=UId,name=Name,surname=Surname} = NewUser) ->
    OldUser = case kvs:get(user,UId) of
        {error,notfound} -> NewUser;
        {ok,#user{}=User} -> User
    end,
    kvs:put(NewUser),
    case Name==OldUser#user.name andalso Surname==OldUser#user.surname of
        true -> ok;
        false -> kvs:update_user_name(UId,Name,Surname)
    end.

subscription_mq(Type, Action, MeId, ToId) ->
    {ok, Channel} = mqs:open([]),
    Routes = case Type of
                 user -> rk_user_feed(ToId);
                 group -> rk_group_feed(ToId)
             end,
    case Action of
        add -> bind_user_exchange(Channel, MeId, Routes);
        remove -> unbind_user_exchange(Channel, MeId, Routes)
    end,
    mqs_channel:close(Channel).

init_mq(User=#user{}) ->
    Groups = groups:list_groups_per_user(User),
    ?INFO("~p init mq. users: ~p", [User, Groups]),
    UserExchange = ?USER_EXCHANGE(User),
    ExchangeOptions = [{type, <<"fanout">>}, durable, {auto_delete, false}],
    {ok, Channel} = mqs:open([]),
    ?INFO("Cration Exchange: ~p,",[{Channel,UserExchange,ExchangeOptions}]),
    mqs_channel:create_exchange(Channel, UserExchange, ExchangeOptions), ?INFO("Created OK"),
    Relations = build_user_relations(User, Groups),
    [bind_user_exchange(Channel, User, RK) || RK <- Relations],
    mqs_channel:close(Channel);

init_mq(Group=#group{}) ->
    GroupExchange = ?GROUP_EXCHANGE(Group),
    ExchangeOptions = [{type, <<"fanout">>}, durable, {auto_delete, false}],
    {ok, Channel} = mqs:open([]),
    ok = mqs_channel:create_exchange(Channel, GroupExchange, ExchangeOptions),
    Relations = build_group_relations(Group),
    [bind_group_exchange(Channel, Group, RK) || RK <- Relations],
    mqs_channel:close(Channel).

build_user_relations(User, Groups) -> [
    rk( [db, user, User, put] ),
    rk( [subscription, user, User, add_to_group]),
    rk( [subscription, user, User, remove_from_group]),
    rk( [subscription, user, User, leave_group]),
    rk( [login, user, User, update_after_login]),
    rk( [likes, user, User, add_like]),
    rk( [feed, delete, User]),
    rk( [feed, user, User, '*', '*', '*']),
    rk( [feed, user, User, count_entry_in_statistics] ),
    rk( [feed, user, User, count_comment_in_statistics] ),
    rk( [feed, user, User, post_note] ),
    rk( [subscription, user, User, subscribe_user]),
    rk( [subscription, user, User, remove_subscribe]),
    rk( [subscription, user, User, set_user_game_status]),
    rk( [subscription, user, User, update_user]),
    rk( [subscription, user, User, block_user]),
    rk( [subscription, user, User, unblock_user]),
    rk( [payment, user, User, set_purchase_external_id]),
    rk( [payment, user, User, set_purchase_state]),
    rk( [payment, user, User, set_purchase_info]),
    rk( [payment, user, User, add]),
    rk( [transaction, user, User, add]),
    rk( [invite, user, User, add]),
    rk( [meeting, user, User, create]),
    rk( [meeting, user, User, join]),
    rk( [purchase, user, User, buy_gift]),
    rk( [purchase, user, User, give_gift]),
    rk( [purchase, user, User, mark_gift_as_deliving]),
    rk( [feed, system, '*', '*']) |
    [rk_group_feed(G) || G <- Groups]
    ].

build_group_relations(Group) -> [
    rk( [db, group, Group, put] ),
    rk( [db, group, Group, update_group] ),
    rk( [db, group, Group, remove_group] ),
    rk( [likes, group, Group, add_like]),   % for comet mostly
    rk( [feed, delete, Group] ),
    rk( [feed, group, Group, '*', '*', '*'] )
    ].

rk_user_feed(User) -> rk([feed, user, User, '*', '*', '*']).
rk_group_feed(Group) -> rk([feed, group, Group, '*', '*', '*']).

bind_user_exchange(Channel, User, RoutingKey) -> {bind, RoutingKey, mqs_channel:bind_exchange(Channel, ?USER_EXCHANGE(User), ?NOTIFICATIONS_EX, RoutingKey)}.
unbind_user_exchange(Channel, User, RoutingKey) -> {unbind, RoutingKey, mqs_channel:unbind_exchange(Channel, ?USER_EXCHANGE(User), ?NOTIFICATIONS_EX, RoutingKey)}.
bind_group_exchange(Channel, Group, RoutingKey) -> {bind, RoutingKey, mqs_channel:bind_exchange(Channel, ?GROUP_EXCHANGE(Group), ?NOTIFICATIONS_EX, RoutingKey)}.
unbind_group_exchange(Channel, Group, RoutingKey) -> {unbind, RoutingKey, mqs_channel:unbind_exchange(Channel, ?GROUP_EXCHANGE(Group), ?NOTIFICATIONS_EX, RoutingKey)}.

rk(List) -> mqs_lib:list_to_key(List).

retrieve_connections(Id,Type) ->
    Friends = case Type of 
                  user -> kvs_users:list_subscr_usernames(Id);
                     _ -> groups:list_group_members(Id) end,
    case Friends of
	[] -> [];
	Full -> Sub = lists:sublist(Full, 10),
                case Sub of
                     [] -> [];
                      _ -> Data = [begin case kvs:get(user,Who) of
                                       {ok,User} -> RealName = kvs_users:user_realname_user(User),
                                                    Paid = accounts:user_paid(Who),
                                                    {Who,Paid,RealName};
				               _ -> undefined end end || Who <- Sub],
			   [X||X<-Data, X/=undefined] end end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_notice(["system", "create_group"] = Route, 
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): create_group: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {UId, GId, Name, Desc, Publicity} = Message,
    FId = kvs:feed_create(),
    CTime = erlang:now(),

    Group =#group{username = GId,
                              name = Name,
                              description = Desc,
                              publicity = Publicity,
                              creator = UId,
                              created = CTime,
                              owner = UId,
                              feed = FId},
    kvs:put(Group),
    mqs:notify([group, init], {GId, FId}),
    kvs_users:init_mq(Group),


    {noreply, State};

handle_notice(["db", "group", GId, "remove_group"] = Route, 
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): remove_group: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {_, Group} = groups:get_group(GId),
    case Group of 
        notfound -> ok;
        _ ->
            mqs:notify([feed, delete, GId], empty),
            kvs:delete_by_index(group_subs, <<"group_subs_group_id_bin">>, GId),         
            kvs:delete(feed, Group#group.feed),
            kvs:delete(group, GId),
            % unbind exchange
            {ok, Channel} = mqs:open([]),
            Routes = kvs_users:rk_group_feed(GId),
            kvs_users:unbind_group_exchange(Channel, GId, Routes),
            mqs_channel:close(Channel)
    end,
    {noreply, State};

handle_notice(["subscription", "user", UId, "add_to_group"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): add_to_group: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {GId, Who, UType} = Message,

    case kvs:get(group_subs, {UId, GId}) of
        {error, notfound} ->
            {R, Group} = kvs:get(group, GId),
            case R of 
                error -> ?INFO("Add to group failed reading group");
                _ ->
                    GU = Group#group.users_count,
                    kvs:put(Group#group{users_count = GU+1})
            end;
        _ ->
            ok
    end,

    OK = kvs:put({group_subs,UId,GId,Type,0}),

%    add_to_group(Who, GId, UType),
    ?INFO("add ~p to group ~p with Type ~p by ~p", [Who, GId,UType,UId]),
    kvs_users:subscribemq(group, add, Who, GId),
    {noreply, State};

handle_notice(["subscription", "user", UId, "remove_from_group"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): remove_from_group: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),    
    {GId} = Message,
    ?INFO("remove ~p from group ~p", [UId, GId]),
    kvs_users:remove_subscription_mq(group, UId, GId),

    kvs:delete(group_subs, {UId, GId}),
    {R, Group} = kvs:get(group, GId),
    case R of
        error -> ?INFO("Remove ~p from group failed reading group ~p", [UId, GId]);
        _ ->
            GU = Group#group.users_count,
            kvs:put(Group#group{users_count = GU-1})
    end,

    {noreply, State};

handle_notice(["subscription", "user", UId, "leave_group"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO(" queue_action(~p): leave_group: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {GId} = Message,
    {R, Group} = kvs:get(group, GId),
    case R of 
        error -> ?ERROR(" Error reading group ~p for leave_group", [GId]);
        ok ->
            case Group#group.owner of
                UId -> % User is owner, transfer ownership to someone else
                    Members = groups:list_group_members(GId),
                    case Members of
                        [ FirstOne | _ ] ->
                            ok = kvs:put(Group#group{owner = FirstOne}),
                            mqs:notify(["subscription", "user", UId, "remove_from_group"], {GId});
                        [] ->
                            % Nobody left in group, remove group at all
                            mqs:notify([db, group, GId, remove_group], [])
                    end;
                _ -> % Plain user removes -- just remove it
                    mqs:notify(["subscription", "user", UId, "remove_from_group"], {GId})
            end;
        _ -> % user is just someone, remove it
            mqs:notify(["subscription", "user", UId, "remove_from_group"], {GId})
    end,
    {noreply, State};

handle_notice(["subscription", "user", UId, "subscribe"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO(" queue_action(~p): subscribe_user: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {Whom} = Message,
    kvs_users:subscribe(UId, Whom),
    {noreply, State};

handle_notice(["subscription", "user", UId, "unsubscribe"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO(" queue_action(~p): remove_subscribe: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {Whom} = Message,
    kvs_users:unsubscribe(UId, Whom),
    {noreply, State};

handle_notice(["subscription", "user", _UId, "update"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO(" queue_action(~p): update_user: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {NewUser} = Message,
    kvs_users:update_user(NewUser),
    {noreply, State};

handle_notice(["gifts", "user", UId, "buy_gift"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO(" queue_action(~p): buy_gift: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {GId} = Message,
    kvs_users:buy_gift(UId, GId),
    {noreply, State};

handle_notice(["gifts", "user", UId, "give_gift"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO(" queue_action(~p): give_gift: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {GId} = Message,
    kvs_users:give_gift(UId, GId),
    {noreply, State};

handle_notice(["gifts", "user", UId, "mark_gift_as_deliving"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO(" queue_action(~p): mark_gift_as_deliving: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {GId, GTimestamp} = Message,
    kvs_users:mark_gift_as_deliving(UId, GId, GTimestamp),
    {noreply, State};

handle_notice(["login", "user", UId, "update_after_login"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): update_after_login: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),    
    Update =
        case kvs_users:user_status(UId) of
            {error, status_info_not_found} ->
                #user_status{username = UId,
                             last_login = erlang:now()};
            {ok, UserStatus} ->
                UserStatus#user_status{last_login = erlang:now()}
        end,
    kvs:put(Update),
    {noreply, State};

handle_notice(Route, Message, State) -> error_logger:info_msg("Unknown USERS notice").