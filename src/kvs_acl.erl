-module(kvs_acl).
-copyright('Synrc Research Center s.r.o.').
-compile(export_all).
-include_lib("kvs/include/kvs.hrl").
-include_lib("kvs/include/acls.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feeds.hrl").

init(Backend) ->
    ?CREATE_TAB(acl),
    ?CREATE_TAB(acl_entry),
    ok.

define_access(Accessor, Resource, Action) -> 
    Entry = #acl_entry{ id = {Accessor, Resource},
                        accessor= Accessor,
                        action  = Action,
                        feed_id = Resource},
    case kvs:add(Entry) of {error, exist} -> kvs:put(Entry#acl_entry{action=Action}); {ok, E} -> E end.

check(Keys) ->
    Acls = [Acl || {ok, Acl = #acl_entry{}} <- [kvs:get(acl_entry, Key) || Key <- Keys]],
    case Acls of [] -> none;
        [#acl_entry{action = Action} | _] -> Action end.

check_access(#user{id = UId, type = UType}, #feed{id = FId}) ->
    Feed = {feed, FId},
    Query = [ {{user, UId}, Feed}, {{user_type, UType}, Feed}, {default, Feed}],
    check(Query);

check_access(#user{id = UId, type = UType}, #group{id = GId}) ->
    Group = {group, GId},
    Query = [ {{user, UId}, Group}, {{user_type, UType}, Group}, {default, Group}],
    check(Query);


check_access(#user{id = AId, type = AType}, #user{id = RId}) ->
    User = {user, RId},
    Query = [ {{user, AId}, User}, {{user_type, AType}, User}, {default, User} ],
    check(Query);

check_access({user_type, Type}, #user{id = RId}) ->
    User = {user, RId},
    Query = [ {{user_type, Type}, User}, {default, User} ],
    check(Query);

check_access({user_type, Type}, #feed{id = FId}) ->
    Feed = {feed, FId},
    Query = [ {{user_type, Type}, Feed}, {default, Feed} ],
    check(Query);

check_access({user_type, Type}, #group{id = GId}) ->
    Group = {group, GId},
    Query = [{{user_type, Type}, Group}, {default, Group}],
    check(Query);

check_access({ip, _Ip} = Accessor, {feature, _Feature} = Resource) ->
    Query = [{Accessor, Resource}, {default, Resource}],
    check(Query);

check_access(#user{id = AId, type = AType}, {feature, _Feature} = R) ->
    Query = [ {{user, AId}, R}, {{user_type, AType}, R}, {default, R} ],
    check(Query);

check_access(UId, {feature, _Feature} = Resource) ->
    case kvs:get(user, UId) of
        {ok, User} -> check_access(User, Resource);
        E -> E
    end.