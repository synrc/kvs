-module(kvs_acl).
-copyright('Synrc Research Center s.r.o.').
-compile(export_all).
-include("kvs.hrl").
-include("metainfo.hrl").
-include("acl.hrl").
-include("user.hrl").

metainfo() ->
    #schema{name=kvs,tables=[
        #table{name=acl,container=true,fields=record_info(fields,acl),keys=[id,accessor]},
        #table{name=access,container=acl,fields=record_info(fields,access)}
    ]}.

define_access(Accessor, Resource, Action) ->
    Entry = #access{ id={Accessor, Resource}, accessor=Accessor, action=Action},
    case kvs:add(Entry) of
        {error, exist} -> kvs:put(Entry#access{action=Action});
        {error, no_container} -> skip;
        {ok, E} -> E end.

check(Keys) ->
    Acls = [Acl || {ok, Acl = #access{}} <- [kvs:get(access, Key) || Key <- Keys]],
    case Acls of
        [] -> none;
        [#access{action = Action} | _] -> Action end.

check_access(#user{id = Id}, Feature) ->
    Query = [ {{user,Id},Feature} ],
    check(Query);

check_access(Id, Feature) ->
    case kvs:get(user, Id) of
        {ok, User} -> check_access(User, Feature);
        E -> E end.
