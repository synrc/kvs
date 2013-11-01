-module(kvs_group).
-compile(export_all).
-include_lib("kvs/include/kvs.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/feed_state.hrl").
-include_lib("kvs/include/config.hrl").
-include_lib("mqs/include/mqs.hrl").

init(Backend) ->
    ?CREATE_TAB(group_subscription),
    ?CREATE_TAB(group),
    Backend:add_table_index(group_subscription, who),
    Backend:add_table_index(group_subscription, where),
    ok.

retrieve_groups(User) ->
    case participate(User) of
         [] -> [];
         Gs -> UC_GId = lists:reverse(lists:sort([{members_count(GId), GId} || GId <- Gs])),
               Result = [begin case kvs:get(group,GId) of
                                   {ok, Group} -> {Group#group.name,GId,UC};
                                   _ -> undefined end end || {UC, GId} <- UC_GId],
               [X||X<-Result,X/=undefined] end.

delete(GroupName) ->
    case kvs:get(group,GroupName) of 
        {error,_} -> ok;
        {ok, Group} ->
%            mqs:notify([feed, delete, GroupName], empty),
            kvs:delete_by_index(group_subscription, <<"where_bin">>, GroupName),
            [kvs:delete(Feed, Fid) || {Feed, Fid} <- Group#group.feeds],
            kvs:delete(group, GroupName),
            case mqs:open([]) of
                {ok, Channel} ->
                    Routes = kvs_users:rk_group_feed(GroupName),
                    kvs_users:unbind_group_exchange(Channel, GroupName, Routes),
                    mqs_channel:close(Channel);
                {error,Reason} -> error_logger:info_msg("delete group failed: ~p",[Reason]) end end.

participate(UserName) -> DBA=?DBA,DBA:participate(UserName).
members(GroupName) -> DBA=?DBA,DBA:members(GroupName).

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
        {error, _} -> not_in_group;
        {ok, #group_subscription{type=Type}} -> Type end.

add(UserName, GroupName, Type) ->
    kvs:put(#group_subscription{key={UserName,GroupName},who=UserName, where=GroupName, type=Type}),
    {ok, Group} = kvs:get(group, GroupName),
    Users = Group#group.users_count,
    kvs:put(Group#group{users_count = Users + 1}).

join(UserName,GroupName) ->
    case kvs:get(group,GroupName) of
        {ok, #group{id = GroupName, scope = public}} ->
            add(UserName, GroupName, member),
            {ok, joined};
        {ok, #group{id = GroupName}} ->
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
        {error,_} -> error_logger:info_msg("Remove ~p from group failed reading group ~p", [UserName, GroupName]);
        {ok,Group} -> kvs:put(Group#group{users_count = Group#group.users_count - 1}) end.

approve_request(UserName, GroupName) -> add(UserName, GroupName, member).
reject_request(UserName, GroupName) -> add(UserName, GroupName, req_rejected).
member_type(UserName, GroupName, Type) -> msg:notify(["subscription", "user", GroupName, "add_to_group"], {GroupName, UserName, Type}).

exists(GroupName) -> case kvs:get(group,GroupName) of {ok,_} -> true; _ -> false end.

coalesce(undefined, B) -> B;
coalesce(A, _) -> A.

publicity(GroupName) ->
    case kvs:get(group,GroupName) of
        {error,_} -> no_such_group;
        {ok,Group} -> Group#group.scope end.

members_count(GroupName) ->
    case kvs:get(group,GroupName) of
        {error,_} -> no_such_group;
        {ok,Group} -> Group#group.users_count end.

user_has_access(UserName, GroupName) ->
    Type = member_type(UserName, GroupName),
    case kvs:get(group, GroupName) of
        {error,_} -> false;
        {ok,Group} ->
            Publicity = Group#group.scope,
            case {Publicity, Type} of
                {public, _} -> true;
                {private, member} -> true;
                _ -> false end end.

handle_notice([kvs_group, Owner, add],
              [#group{}=Group],
              #state{owner=Owner}=State) ->
    error_logger:info_msg("[kvs_group] Create group ~p", [Owner]),
    Created = case kvs:add(Group) of {error, E} -> {error, E};
    {ok, #group{id=Id} = G} ->
        Params = [{id, Id}, {type, group}, {feeds, element(#iterator.feeds, G)}],
        case workers_sup:start_child(Params) of {error, E} -> {error, E}; _ -> G end end,

    msg:notify([kvs_group, group, Group#group.id, added], [Created]),
    {noreply, State};


handle_notice(["kvs_group", "update", GroupName] = Route, 
    Message, #state{owner=ThisGroupOwner, type=Type} = State) ->
    error_logger:info_msg("queue_action(~p): update_group: Owner=~p, Route=~p, Message=~p", [self(), {Type, ThisGroupOwner}, Route, Message]),    
    {_UId, _GroupUsername, Name, Description, Owner, Publicity} = Message,
    case kvs:get(group, GroupName) of
        {ok, Group} ->
            NewGroup = Group#group{name = coalesce(Name,Group#group.name),
                description = coalesce(Description,Group#group.description),
                scope = coalesce(Publicity,Group#group.scope),
                owner = coalesce(Owner,Group#group.owner)},
            kvs:put(NewGroup);
        {error,Reason} -> error_logger:info_msg("Cannot update group ~p",[Reason]) end,
    {noreply, State};

handle_notice([kvs_group, Owner, delete],
              [#group{}=P],
              #state{owner=Owner, feeds=Feeds}=State) ->
    error_logger:info_msg("[kvs_group] Delete group ~p ~p", [P#group.id, Owner]),
    Removed = case kvs:remove(P) of {error,E} -> {error,E};
    ok ->
        % todo: handle group feeds?

        supervisor:terminate_child(workers_sup, {group, P#group.id}),
        supervisor:delete_child(workers_sup, {group, P#group.id}),
        P
    end,
    msg:notify([kvs_product, group, P#group.id, deleted], [Removed]),
    {noreply, State};

handle_notice([kvs_group, join, Owner],
              [{FeedName, Who}, Entry],
              #state{owner = Owner, feeds=Feeds} = State) ->
    error_logger:info_msg("[kvs_group] ~p Join group:  ~p", [Who, Owner]),
    join(Who, Owner),
    case lists:keyfind(FeedName, 1, Feeds) of false -> skip;
    {_,Fid} -> msg:notify([kvs_feed, group, Owner, entry, Who, add],
                          [Entry#entry{id={Who, Fid},feed_id=Fid,to={group, Owner}}]) end,

%    subscription_mq(group, add, UserName, GroupName),
    {noreply, State};

handle_notice([kvs_group, leave, Owner],
              [{FeedName, Who}],
              #state{owner = Owner, feeds=Feeds} = State) ->
    error_logger:info_msg("[kvs_group] ~p leave group ~p", [Who, Owner]),
    leave(Who, Owner),
    case lists:keyfind(FeedName, 1, Feeds) of false -> skip;
    {_,Fid} -> msg:notify([kvs_feed, group, Owner, entry, delete], [#entry{id={Who,Fid}, feed_id=Fid}]) end,

%    subscription_mq(group, remove, UserName, GroupName),
    {noreply, State};

handle_notice(_Route, _Message, State) ->
%  error_logger:info_msg("Unknown GROUP notice"),
  {noreply, State}.

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
    GroupExchange = ?GROUP_EXCHANGE(Group#group.id),
    ExchangeOptions = [{type, <<"fanout">>}, durable, {auto_delete, false}],
    case mqs:open([]) of
        {ok, Channel} ->
            mqs_channel:create_exchange(Channel, GroupExchange, ExchangeOptions),
            Relations = build_group_relations(Group),
            [mqs_channel:bind_exchange(Channel, ?GROUP_EXCHANGE(Group#group.id), ?NOTIFICATIONS_EX, Route) || Route <- Relations],
            mqs_channel:close(Channel);
        {error, Reason} -> error_logger:info_msg("init_mq error: ~p",[Reason]) end.

rk_group_feed(Group) -> mqs_lib:list_to_key([kvs_feed, group, Group, '*', '*', '*']).

subscription_mq(Type, Action, Who, Where) ->
    case mqs:open([]) of
        {ok,Channel} ->
            case {Type,Action} of 
                {group,add}     -> mqs_channel:bind_exchange(Channel, ?GROUP_EXCHANGE(Who), ?NOTIFICATIONS_EX, rk_group_feed(Where));
                {groupr,remove}  -> mqs_channel:unbind_exchange(Channel, ?GROUP_EXCHANGE(Who), ?NOTIFICATIONS_EX, rk_group_feed(Where)) end,
            mqs_channel:close(Channel);
        {error,Reason} -> error_logger:info_msg("subscription_mq error: ~p",[Reason]) end.
