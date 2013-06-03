-module(kvs_group).
-compile(export_all).
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/log.hrl").

retrieve_groups(User) ->
    ?INFO("retrieve group for user: ~p",[User]),
    case participate(User) of
         [] -> [];
         Gs -> UC_GId = lists:sublist(lists:reverse(
                              lists:sort([{group_members_count(GId), GId} || GId <- Gs])), 
                                    20),
               Result = [begin case get_group(GId) of
                                   {ok, Group} -> {Group#group.name,GId,UC};
                                   _ -> undefined end end || {UC, GId} <- UC_GId],
               [X||X<-Result,X/=undefined] end.

create_group_directly_to_db(UId, GId, Name, Desc, Publicity) ->
    FId = kvs:feed_create(),
    CTime = erlang:now(),
    Group = #group{username = GId,
                      name = Name,
                      description = Desc,
                      publicity = Publicity,
                      creator = UId,
                      created = CTime,
                      owner = UId,
                     feed = FId},
    kvs:put(Group),
    kvs_users:init_mq(Group),
    add_to_group_directly_to_db(UId, GId, member),
    GId.

add_to_group(Who, GId, Type, Owner) -> mqs:notify(["subscription", "user", Owner, "add_to_group"], {GId, Who, Type}).

add_to_group_directly_to_db(UId, GId, Type) ->
    kvs:put(#group_subscription{key={UId,GId},user_id=UId, group_id=GId, user_type=Type}),
    {ok, Group} = kvs:get(group, GId),
    GU = Group#group.users_count,
    kvs:put(Group#group{users_count = GU+1}).

delete_group(GId) ->
    {_, Group} = get_group(GId),
    case Group of 
        notfound -> ok;
        _ ->
            mqs:notify([feed, delete, GId], empty),
            kvs:delete_by_index(group_subscription, <<"group_subs_group_id_bin">>, GId),         
            kvs:delete(feed, Group#group.feed),
            kvs:delete(group, GId),
            % unbind exchange
            {ok, Channel} = mqs:open([]),
            Routes = kvs_users:rk_group_feed(GId),
            kvs_users:unbind_group_exchange(Channel, GId, Routes),
            mqs_channel:close(Channel)
    end.

participate(UId) -> [GId || #group_subscription{group_id=GId} <- kvs:all_by_index(group_subs, <<"group_subs_user_id_bin">>, UId) ].
members(GId) -> [UId || #group_subscription{user_id=UId, user_type=UT} <- kvs:all_by_index(group_subs, <<"group_subs_group_id_bin">>, GId), UT == member ].
members_by_type(GId, Type) -> [UId || #group_subscription{user_id=UId, user_type=UT} <- kvs:all_by_index(group_subs, <<"group_subs_group_id_bin">>, GId), UT == Type ].
members_with_types(GId) -> [{UId, UType} || #group_subscription{user_id=UId, user_type=UType} <- kvs:all_by_index(group_subs, <<"group_subs_group_id_bin">>, list_to_binary(GId)) ].

get_group(GId) -> kvs:get(group, GId).

user_is_owner(UId, GId) ->
    {R, Group} = kvs:get(group, GId),
    case R of
        ok -> case Group#group.owner of
                UId -> true;
                _ -> false
            end;
        _ -> false
    end.

user_in_group(UId, GId) ->
    case kvs:get(group_subs, {UId, GId}) of
        {error, notfound} -> false;
        _ -> true
    end.

group_user_type(UId, GId) ->
    case kvs:get(group_subs, {UId, GId}) of
        {error, notfound} -> not_in_group;
        {ok, #group_subscription{user_type=Type}} -> Type
    end.

join_group(GId, User) ->
    {ok, Group} = get_group(GId),
    case Group of
        #group{username = GId, publicity = public} ->
            % Join to this group
            add_to_group(User, GId, member, Group#group.owner),
            {ok, joined};
        #group{username = GId, publicity = _, feed=_Feed} ->
            case group_user_type(User, GId) of
                member -> {ok, joined};
                req -> {error, already_sent};
                reqrejected -> {error, request_rejected};
                not_in_group -> add_to_group(User, GId, req, Group#group.owner), {ok, requested};
                _ -> {error, unknown_type}
            end;
        _ -> {error, notfound}
    end.

approve_request(UId, GId, Owner) -> add_to_group(UId, GId, member, Owner).
reject_request(UId, GId, Owner) -> add_to_group(UId, GId, reqrejected, Owner).
change_group_user_type(UId, GId, Type) -> mqs:notify(["subscription", "user", UId, "add_to_group"], {GId, UId, Type}).

group_exists(GId) ->
    {R, _} = get_group(GId),
    case R of
        ok -> true;
        _ -> false
    end.

group_publicity(GId) ->
    {_, Group} = get_group(GId),
    case Group of
        notfound ->
            no_such_group;
        _ ->
            Group#group.publicity
    end.

group_members_count(GId) ->
    {_, Group} = get_group(GId),
    case Group of
        notfound ->
            no_such_group;
        _ ->
            Group#group.users_count
    end.

user_has_access(UId, GId) ->
    UType = group_user_type(UId, GId),
    {_, Group} = get_group(GId),
    case Group of
        notfound ->
            false;
        _ ->
            GPublicity = Group#group.publicity,
            case {GPublicity, UType} of
                {public, _} -> true;
                {private, member} -> true;
                _ -> false
            end
    end.
