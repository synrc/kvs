-module(kvs_user).
-copyright('Synrc Research Center s.r.o.').
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/purchases.hrl").
-include_lib("kvs/include/payments.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/products.hrl").
-include_lib("kvs/include/config.hrl").
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/feed_state.hrl").
-include_lib("mqs/include/mqs.hrl").
-include_lib("kvs/include/kvs.hrl").
-compile(export_all).

init(Backend) ->
    ?CREATE_TAB(user),
    ?CREATE_TAB(user_status),
    ?CREATE_TAB(subscription),
    Backend:add_table_index(user, facebook_id),
    Backend:add_table_index(user, googleplus_id),
    Backend:add_table_index(user, twitter_id),
    Backend:add_table_index(user, github_id),
    Backend:add_table_index(subscription, who),
    Backend:add_table_index(subscription, whom),
    ok.

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
get({googleplus, GId}) -> user_by_googleplus_id(GId).

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
        {error,Reason} -> error_logger:info_msg("subscription_mq error: ~p",[Reason]) end.

init_mq(User=#user{}) ->
    Groups = kvs_group:participate(User),
    UserExchange = ?USER_EXCHANGE(User#user.username),
    ExchangeOptions = [{type, <<"fanout">>}, durable, {auto_delete, false}],
    case mqs:open([]) of
        {ok, Channel} ->
            error_logger:info_msg("Cration Exchange: ~p,",[{Channel,UserExchange,ExchangeOptions}]),
            mqs_channel:create_exchange(Channel, UserExchange, ExchangeOptions),
            Relations = build_user_relations(User, Groups),
            [ mqs_channel:bind_exchange(Channel, ?USER_EXCHANGE(User#user.username), ?NOTIFICATIONS_EX, Route) || Route <- Relations],
            mqs_channel:close(Channel);
        {error,Reason} -> error_logger:info_msg("init_mq error: ~p",[Reason]) end.

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

handle_notice([kvs_user, user, registered], {_,_,#user{id=Who}=U}, #state{owner=Who}=State)->
    error_logger:info_msg("Notification about registered me ~p", [U]),
    kvs_account:create_account(Who),
    {ok, DefaultQuota} = kvs:get(config, "accounts/default_quota",  300),
    kvs_account:transaction(Who, quota, DefaultQuota, #tx_default_assignment{}),
    {noreply, State};

handle_notice([kvs_user, user, Owner, delete],
              [#user{}=U],
              #state{owner=Owner}=State) ->
    error_logger:info_msg("[kvs_user]Delete user ~p", [U]),
    {noreply, State};

handle_notice([kvs_user, login, user, Who, update_status],
              {Status},
              #state{owner=Who} = State) ->
    error_logger:info_msg("[kvs_user] Update ~p status ~p", [Who, Status]),
    case kvs:get(user, Who) of {error,_}-> ok;
    {ok, U}-> kvs:put(U#user{status=Status}) end,

    Update = case kvs:get(user_status, Who) of
    {error, not_found} -> #user_status{email = Who, last_login = erlang:now()};
    {ok, UserStatus} -> UserStatus#user_status{last_login = erlang:now()} end,
    kvs:put(Update),

    {noreply, State};

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

handle_notice(Route, Message, State) -> 
  %error_logger:info_msg("Unknown USERS notice"), 
  {noreply, State}.
