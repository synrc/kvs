-module(kvs_group).
-compile(export_all).
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/log.hrl").
-include_lib("kvs/include/feed_state.hrl").
-include_lib("mqs/include/mqs.hrl").

retrieve_groups(User) ->
    ?INFO("retrieve group for user: ~p",[User]),
    case participate(User) of
         [] -> [];
         Gs -> UC_GId = lists:sublist(lists:reverse(
                              lists:sort([{members_count(GId), GId} || GId <- Gs])), 
                                    20),
               Result = [begin case kvs:get(group,GId) of
                                   {ok, Group} -> {Group#group.name,GId,UC};
                                   _ -> undefined end end || {UC, GId} <- UC_GId],
               [X||X<-Result,X/=undefined] end.

create(Creator, GroupName, GroupFullName, Desc, Publicity) ->
    Feed = kvs_feed:create(),
    Time = erlang:now(),
    Group = #group{username = GroupName, name = GroupFullName, description = Desc, publicity = Publicity,
                   creator = Creator, created = Time, owner = Creator, feed = Feed},
    kvs:put(Group),
    init_mq(Group),
    mqs:notify([group, init], {GroupName, Feed}),
    add(Creator, GroupName, member),
    GroupName.


delete(GroupName) ->
    case kvs:get(group,GroupName) of 
        {error,_} -> ok;
        {ok, Group} ->
            mqs:notify([feed, delete, GroupName], empty),
            kvs:delete_by_index(group_subscription, <<"where_bin">>, GroupName),
            kvs:delete(feed, Group#group.feed),
            kvs:delete(group, GroupName),
            case mqs:open([]) of
                {ok, Channel} ->
                    Routes = kvs_users:rk_group_feed(GroupName),
                    kvs_users:unbind_group_exchange(Channel, GroupName, Routes),
                    mqs_channel:close(Channel);
                {error,Reason} -> ?ERROR("delete group failed: ~p",[Reason]) end end.

participate(UserName) -> [GroupName || #group_subscription{group_id=GroupName} <- kvs:all_by_index(group_subscription, <<"who_bin">>, UserName) ].
members(GroupName) -> [UserName || #group_subscription{user_id=UserName} <- kvs:all_by_index(group_subscription, <<"where_bin">>, GroupName) ].
members_by_type(GroupName, Type) -> [UserName || #group_subscription{user_id=UserName, user_type=T} <- kvs:all_by_index(group_subscription, <<"where_bin">>, GroupName), T == Type ].
members_with_types(GroupName) -> [{UserName, Type} || #group_subscription{user_id=UserName, user_type=Type} <- kvs:all_by_index(group_subscriptioin, <<"where_bin">>, list_to_binary(GroupName)) ].

owner(UserName, GroupName) ->
    case kvs:get(group, GroupName) of
        {ok,Group} -> case Group#group.owner of UserName -> true;  _ -> false end;
        _ -> false end.

member(UserName, GroupName) ->
    case kvs:get(group_subscription, {UserName, GroupName}) of
        {error, _} -> false;
        {ok,_} -> true end.

member_type(UserName, GroupName) ->
    case kvs:get(group_subs, {UserName, GroupName}) of
        {error, notfound} -> not_in_group;
        {ok, #group_subscription{user_type=Type}} -> Type end.

add(UserName, GroupName, Type) ->
    kvs:put(#group_subscription{key={UserName,GroupName},user_id=UserName, group_id=GroupName, user_type=Type}),
    {ok, Group} = kvs:get(group, GroupName),
    Users = Group#group.users_count,
    kvs:put(Group#group{users_count = Users + 1}).

join(UserName,GroupName) ->
    case kvs:get(group,GroupName) of
        {ok, #group{username = GroupName, publicity = public}} ->
            add(UserName, GroupName, member),
            {ok, joined};
        {ok, #group{username = GroupName}} ->
            case member_type(UserName, GroupName) of
                member -> {ok, joined};
                req -> {error, already_sent};
                req_rejected -> {error, request_rejected};
                not_in_group -> add(UserName, GroupName, req), {ok, requested};
                _ -> {error, unknown_type} end;
        Error -> Error end.

leave(UserName,GroupName) ->
    kvs:delete(group_subscription, {UserName, GroupName}),
    case kvs:get(group, GroupName) of
        {error,_} -> ?ERROR("Remove ~p from group failed reading group ~p", [UserName, GroupName]);
        {ok,Group} -> kvs:put(Group#group{users_count = Group#group.users_count - 1}) end.

approve_request(UserName, GroupName) -> add(UserName, GroupName, member).
reject_request(UserName, GroupName) -> add(UserName, GroupName, req_rejected).
member_type(UserName, GroupName, Type) -> mqs:notify(["subscription", "user", GroupName, "add_to_group"], {GroupName, UserName, Type}).

exists(GroupName) -> case kvs:get(group,GroupName) of {ok,_} -> true; _ -> false end.

coalesce(undefined, B) -> B;
coalesce(A, _) -> A.

publicity(GroupName) ->
    case kvs:get(group,GroupName) of
        {error,_} -> no_such_group;
        {ok,Group} -> Group#group.publicity end.

members_count(GroupName) ->
    case kvs:get(group,GroupName) of
        {error,_} -> no_such_group;
        {ok,Group} -> Group#group.users_count end.

user_has_access(UserName, GroupName) ->
    Type = member_type(UserName, GroupName),
    case kvs:get(group, GroupName) of
        {error,_} -> false;
        {ok,Group} ->
            Publicity = Group#group.publicity,
            case {Publicity, Type} of
                {public, _} -> true;
                {private, member} -> true;
                _ -> false end end.

handle_notice(["kvs_group", "create"] = Route, 
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): create_group: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {Creator, GroupName, FullName, Desc, Publicity} = Message,
    create(Creator, GroupName, FullName, Desc, Publicity),
    {noreply, State};

handle_notice(["kvs_group", "update", GroupName] = Route, 
    Message, #state{owner=ThisGroupOwner, type=Type} = State) ->
    ?INFO("queue_action(~p): update_group: Owner=~p, Route=~p, Message=~p", [self(), {Type, ThisGroupOwner}, Route, Message]),    
    {_UId, _GroupUsername, Name, Description, Owner, Publicity} = Message,
    case kvs:get(group, GroupName) of
        {ok, Group} ->
            NewGroup = Group#group{name = coalesce(Name,Group#group.name),
                description = coalesce(Description,Group#group.description),
                publicity = coalesce(Publicity,Group#group.publicity),
                owner = coalesce(Owner,Group#group.owner)},
            kvs:put(NewGroup);
        {error,Reason} -> ?ERROR("Cannot update group ~p",[Reason]) end,
    {noreply, State};

handle_notice(["kvs_group", "remove", GroupName] = Route, 
    Message, #state{owner = Owner, type = Type} = State) ->
    ?INFO("queue_action(~p): remove_group: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    delete(GroupName),
    {noreply, State};

handle_notice(["kvs_group", "join", GroupName] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    {GroupName, UserName, Type} = Message,
    join(UserName, GroupName),
    subscription_mq(group, add, UserName, GroupName),
    {noreply, State};

handle_notice(["kvs_group", "leave", GroupName] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): remove_from_group: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {UserName} = Message,
    leave(UserName,GroupName),
    subscription_mq(group, remove, UserName, GroupName),
    {noreply, State};

handle_notice(Route, Message, State) -> error_logger:info_msg("Unknown GROUP notice").

build_group_relations(Group) -> [
    mqs:key( [kvs_group, create] ),
    mqs:key( [kvs_group, update, Group] ),
    mqs:key( [kvs_group, remove, Group] ),
    mqs:key( [kvs_group, join, Group] ),
    mqs:key( [kvs_group, leave, Group] ),
    mqs:key( [kvs_group, like, Group]),   % for comet mostly
    mqs:key( [kvs_feed, delete, Group] ),
    mqs:key( [kvs_feed, group, Group, '*', '*', '*'] )
    ].

init_mq(Group=#group{}) ->
    GroupExchange = ?GROUP_EXCHANGE(Group#group.username),
    ExchangeOptions = [{type, <<"fanout">>}, durable, {auto_delete, false}],
    case mqs:open([]) of
        {ok, Channel} ->
            mqs_channel:create_exchange(Channel, GroupExchange, ExchangeOptions),
            Relations = build_group_relations(Group),
            [bind_group_exchange(Channel, Group, RK) || RK <- Relations],
            mqs_channel:close(Channel);
        {error, Reason} -> ?ERROR("init_mq error: ~p",[Reason]) end.

rk_group_feed(Group) -> mqs_lib:list_to_key([feed, group, Group, '*', '*', '*']).

bind_group_exchange(Channel, Group, Route) -> {bind, Route, mqs_channel:bind_exchange(Channel, ?GROUP_EXCHANGE(Group), ?NOTIFICATIONS_EX, Route)}.
unbind_group_exchange(Channel, Group, Route) -> {unbind, Route, mqs_channel:unbind_exchange(Channel, ?GROUP_EXCHANGE(Group), ?NOTIFICATIONS_EX, Route)}.

subscription_mq(Type, Action, MeId, ToId) ->
    case mqs:open([]) of
        {ok,Channel} ->
            case {Type,Action} of 
                {group,add} -> bind_group_exchange(Channel, MeId, rk_group_feed(ToId));
                {group,remove} -> unbind_group_exchange(Channel, MeId, rk_group_feed(ToId)) end,
            mqs_channel:close(Channel);
        {error,Reason} -> ?ERROR("subscription_mq error: ~p",[Reason]) end.
