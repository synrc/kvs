-module(acls).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').
-compile(export_all).

-include_lib("kvs/include/acls.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feeds.hrl").

define_access(default  = Accessor, Resource, Action) -> do_define_access(Accessor, Resource, Action);
define_access({user, _Username} = Accessor, Resource, Action) -> do_define_access(Accessor, Resource, Action);
define_access({user_type, _Usertype} = Accessor, Resource, Action) -> do_define_access(Accessor, Resource, Action);
define_access({ip, _Ip} = Accessor, Resource, Action) -> do_define_access(Accessor, Resource, Action).

do_define_access(Accessor, Resource, Action) -> kvs:acl_add_entry(select_type(Resource), Accessor, Action).

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
    case users:get_user(UId) of
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
