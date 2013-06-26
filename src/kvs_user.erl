-module(kvs_user).
-copyright('Synrc Research Center s.r.o.').
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/log.hrl").
-include_lib("kvs/include/config.hrl").
-include_lib("kvs/include/feed_state.hrl").
-include_lib("mqs/include/mqs.hrl").
-compile(export_all).

register(#user{username=UserName, email=Email, facebook_id = FacebookId, googleplus_id=GooglePlusId} = Registeration) ->
    EmailUser = case check_username(UserName, FacebookId, GooglePlusId) of
        {error, Reason} -> {error, Reason};
        {ok, Name} -> case kvs_user:get({email, Email}) of
            {error, _} -> {ok, Name};
            {ok, _} -> {error, email_taken} end end,

    GroupUser = case EmailUser of
        {error, Reason2} -> {error, Reason2};
        {ok, Name2} -> case kvs:get(group, Name2) of
            {error, _} -> {ok, Name2};
            {ok,_} -> {error, username_taken} end end,

    case GroupUser of
        {ok, Name3} -> process_register(Registeration#user{username=Name3});
        Error -> Error end.

process_register(#user{username=U} = RegisterData0) ->
    HashedPassword = case RegisterData0#user.password of
        undefined -> undefined;
        PlainPassword -> kvs:sha(PlainPassword) end,
    RegisterData = RegisterData0#user {
        feed     = kvs_feed:create(),
        direct   = kvs_feed:create(),
        pinned   = kvs_feed:create(),
        starred  = kvs_feed:create(),
        password = HashedPassword },
    error_logger:info_msg("PUT USER ~p", [U]),
    kvs:put(RegisterData),
    kvs_account:create_account(U),
    {ok, DefaultQuota} = kvs:get(config, "accounts/default_quota",  300),
    kvs_account:transaction(U, quota, DefaultQuota, #tx_default_assignment{}),
    init_mq(RegisterData),
    mqs:notify([user, init], {U, RegisterData#user.feed}),
    {ok, U}.

check_username(Name, Fbid, Gid) ->
    case kvs_user:get(Name) of
        {error, _} -> {ok, Name};
        {ok, User} when Fbid =/= undefined orelse Gid =/= undefined->
            check_username(User#user.username  ++ integer_to_list(crypto:rand_uniform(0,10)), Fbid, Gid);
        {ok, _}-> {error, username_taken} end.

delete(UserName) ->
    case kvs_user:get(UserName) of
        {ok, User} ->
            GIds = kvs_group:participate(UserName),
            [ mqs:notify(["subscription", "user", UserName, "remove_from_group"], {GId}) || GId <- GIds ],
            F2U = [ {MeId, FrId} || #subscription{who = MeId, whom = FrId} <- subscriptions(User) ],
            [ unsubscribe(MeId, FrId) || {MeId, FrId} <- F2U ],
            [ unsubscribe(FrId, MeId) || {MeId, FrId} <- F2U ],
            kvs:delete(user_status, UserName),
            kvs:delete(user, UserName),
            {ok, User};
        E -> E end.

get({facebook, FBId}) -> user_by_facebook_id(FBId);
get({googleplus, GId}) -> user_by_googleplus_id(GId);
get({email, Email}) -> user_by_email(Email);
get(UId) -> kvs:get(user, UId).

subscribe(Who, Whom) ->
    Record = #subscription{key={Who,Whom}, who = Who, whom = Whom},
    kvs:put(Record).

unsubscribe(Who, Whom) ->
    case subscribed(Who, Whom) of
        true  -> kvs:delete(subscription, {Who, Whom});
        false -> skip end.

subscriptions(undefined)-> [];
subscriptions(#user{username = UId}) -> subscriptions(UId);

subscriptions(UId) -> DBA=?DBA, DBA:subscriptions(UId).
subscribed(Who) -> DBA=?DBA, DBA:subscribed(Who).

subscribed(Who, Whom) ->
    case kvs:get(subscription, {Who, Whom}) of
        {ok, _} -> true;
        _ -> false end.

subscription_mq(Type, Action, Who, Whom) ->
    case mqs:open([]) of
        {ok,Channel} ->
            case {Type,Action} of 
                {user,add}     -> mqs_channel:bind_exchange(Channel, ?USER_EXCHANGE(Who), ?NOTIFICATIONS_EX, rk_user_feed(Whom));
                {user,remove}  -> mqs_channel:unbind_exchange(Channel, ?USER_EXCHANGE(Who), ?NOTIFICATIONS_EX, rk_user_feed(Whom)) end,
            mqs_channel:close(Channel);
        {error,Reason} -> ?ERROR("subscription_mq error: ~p",[Reason]) end.

init_mq(User=#user{}) ->
    Groups = kvs_group:participate(User),
    UserExchange = ?USER_EXCHANGE(User#user.username),
    ExchangeOptions = [{type, <<"fanout">>}, durable, {auto_delete, false}],
    case mqs:open([]) of
        {ok, Channel} ->
            ?INFO("Cration Exchange: ~p,",[{Channel,UserExchange,ExchangeOptions}]),
            mqs_channel:create_exchange(Channel, UserExchange, ExchangeOptions),
            Relations = build_user_relations(User, Groups),
            [ mqs_channel:bind_exchange(Channel, ?USER_EXCHANGE(User#user.username), ?NOTIFICATIONS_EX, Route) || Route <- Relations],
            mqs_channel:close(Channel);
        {error,Reason} -> ?ERROR("init_mq error: ~p",[Reason]) end.

build_user_relations(User, Groups) -> [
    mqs:key( [kvs_user, '*', User]),
    mqs:key( [kvs_feed, user, User, '*', '*', '*']),
    mqs:key( [kvs_feed, user, User, '*'] ),
    mqs:key( [kvs_payment, user, User, '*']),
    mqs:key( [kvs_account, user, User, '*']),
    mqs:key( [kvs_meeting, user, User, '*']),
    mqs:key( [kvs_purchase, user, User, '*']) |
  [ mqs:key( [kvs_feed, group, G, '*', '*', '*']) || G <- Groups ]
    ].

rk_user_feed(User) -> mqs:key([kvs_feed, user, User, '*', '*', '*']).

retrieve_connections(Id,Type) ->
    Friends = case Type of 
        user -> kvs_user:list_subscr_usernames(Id);
        _ -> kvs_group:list_group_members(Id) end,
    case Friends of
        [] -> [];
        Full -> Sub = lists:sublist(Full, 10),
            case Sub of
                [] -> [];
                _ ->
                    Data = [begin
                        case kvs:get(user,Who) of
                            {ok,User} -> RealName = kvs_user:user_realname_user(User),
                            Paid = kvs_payment:user_paid(Who),
                            {Who,Paid,RealName};
                        _ -> undefined end end || Who <- Sub],
                    [ X || X <- Data, X/=undefined ] end end.

user_by_facebook_id(FBId) ->
    case kvs:get(facebook,FBId) of
        {ok,{_,User,_}} -> kvs:get(user,User);
        Else -> Else end.

user_by_googleplus_id(GId) ->
    case kvs:get(googleplus,GId) of
        {ok,{_,User,_}} -> kvs:get(user,User);
        Else -> Else end.

user_by_email(Email) ->
    case kvs:get(email,Email) of
        {ok,{_,User,_}} -> kvs:get(user,User);
        Else -> Else end.

handle_notice(["kvs_user", "subscribe", Who] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    {Whom} = Message,
    kvs_user:subscribe(Who, Whom),
    subscription_mq(user, add, Who, Whom),
    {noreply, State};

handle_notice(["kvs_user", "unsubscribe", Who] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    {Whom} = Message,
    kvs_user:unsubscribe(Who, Whom),
    subscription_mq(user, remove, Who, Whom),
    {noreply, State};

handle_notice(["kvs_user", "update", Who] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    {NewUser} = Message,
    kvs:put(NewUser),
    {noreply, State};

handle_notice(Route, Message, State) -> error_logger:info_msg("Unknown USERS notice").
