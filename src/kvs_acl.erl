-module(kvs_acl).
-copyright('Synrc Research Center s.r.o.').
-compile(export_all).

-include_lib("kvs/include/acls.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feeds.hrl").

define_access(default  = Accessor, Resource, Action) -> do_define_access(Accessor, Resource, Action);
define_access({user, _Username} = Accessor, Resource, Action) -> do_define_access(Accessor, Resource, Action);
define_access({user_type, _Usertype} = Accessor, Resource, Action) -> do_define_access(Accessor, Resource, Action);
define_access({ip, _Ip} = Accessor, Resource, Action) -> do_define_access(Accessor, Resource, Action).

do_define_access(Accessor, Resource, Action) -> acl_add_entry(select_type(Resource), Accessor, Action).

check(Keys) ->
    Acls = [Acl || {ok, Acl = #acl_entry{}} <- [kvs:get(acl_entry, Key) || Key <- Keys]],
    case Acls of
        [] -> none;
        [#acl_entry{action = Action} | _] -> Action end.

check_access(#user{username = UId, type = UType}, #feed{id = FId}) ->
    Feed = {feed, FId},
    Query = [ {{user, UId}, Feed}, {{user_type, UType}, Feed}, {default, Feed}],
    check(Query);

check_access(#user{username = UId, type = UType}, #group{username = GId}) ->
    Group = {group, GId},
    Query = [ {{user, UId}, Group}, {{user_type, UType}, Group}, {default, Group}],
    check(Query);


check_access(#user{username = AId, type = AType}, #user{username = RId}) ->
    User = {user, RId},
    Query = [ {{user, AId}, User}, {{user_type, AType}, User}, {default, User} ],
    check(Query);

check_access({user_type, Type}, #user{username = RId}) ->
    User = {user, RId},
    Query = [ {{user_type, Type}, User}, {default, User} ],
    check(Query);

check_access({user_type, Type}, #feed{id = FId}) ->
    Feed = {feed, FId},
    Query = [ {{user_type, Type}, Feed}, {default, Feed} ],
    check(Query);

check_access({user_type, Type}, #group{username = GId}) ->
    Group = {group, GId},
    Query = [{{user_type, Type}, Group}, {default, Group}],
    check(Query);

check_access({ip, _Ip} = Accessor, {feature, _Feature} = Resource) ->
    Query = [{Accessor, Resource}, {default, Resource}],
    check(Query);

check_access(#user{username = AId, type = AType}, {feature, _Feature} = R) ->
    Query = [ {{user, AId}, R}, {{user_type, AType}, R}, {default, R} ],
    check(Query);

check_access(UId, {feature, _Feature} = Resource) ->
    case kvs_users:get_user(UId) of
        {ok, User} -> check_access(User, Resource);
        E -> E
    end.

select_type(#user{username = UId}) -> {user, UId};
select_type(#group{username = GId}) -> {group, GId};
select_type(#feed{id = FId}) -> {feed, FId};
select_type({user, UId}) -> {user, UId};
select_type({group, name = GId}) -> {group, GId};
select_type({feed, FId}) -> {feed, FId};
select_type({feature, Feature}) ->  {feature, Feature}.

acl_entries(AclId) ->
    [AclStr] = io_lib:format("~p",[AclId]),
    RA = kvs:get(acl, erlang:list_to_binary(AclStr)),
    case RA of
        {ok,RO} -> riak_read_acl_entries(RO#acl.top, []);
        {error, notfound} -> [] end.

riak_read_acl_entries(undefined, Result) -> Result;
riak_read_acl_entries(Next, Result) ->
    NextStr = io_lib:format("~p",[Next]),
    RA = kvs:get(acl_entry,erlang:list_to_binary(NextStr)),
    case RA of
         {ok,RO} -> riak_read_acl_entries(RO#acl_entry.prev, Result ++ [RO]);
         {error,notfound} -> Result end.

acl_add_entry(Resource, Accessor, Action) ->
    Acl = case kvs:get(acl, Resource) of
              {ok, A} ->
                  A;
              %% if acl record wasn't created already
              {error, _} ->
                  A = #acl{id = Resource, resource=Resource},
                  kvs:put(A),
                  A
          end,

    EntryId = {Accessor, Resource},

    case kvs:get(acl_entry, EntryId) of
        %% there is no entries for specified Acl and Accessor, we have to add it
        {error, _} ->
            Next = undefined,
            Prev = case Acl#acl.top of
                       undefined ->
                           undefined;

                       Top ->
                           case kvs:get(acl_entry, Top) of
                               {ok, TopEntry} ->
                                   EditedEntry = TopEntry#acl_entry{next = EntryId},
                                   kvs:put(EditedEntry), % update prev entry
                                   TopEntry#acl_entry.id;

                               {error, _} ->
                                   undefined
                           end
                   end,

            %% update acl with top of acl entries list
            kvs:put(Acl#acl{top = EntryId}),

            Entry  = #acl_entry{id = EntryId,
                                entry_id = EntryId,
                                accessor = Accessor,
                                action = Action,
                                next = Next,
                                prev = Prev},

            ok = kvs:put(Entry),
            Entry;

        %% if acl entry for Accessor and Acl is defined - just change action
        {ok, AclEntry} ->
            kvs:put(AclEntry#acl_entry{action = Action}),
            AclEntry
    end.

