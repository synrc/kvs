-module(kvs_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("kvs/include/entry.hrl").
-compile(export_all).

suite() -> [{timetrap,{seconds,30}}].
all() -> [{group, feed},{group,acl}].
groups() -> [{feed,[entry,comment,user]},
             {acl,[access]}].

init_per_suite(Config) ->
    application:start(mnesia),
    application:start(kvs),
    application:set_env(kvs, schema, [kvs_user, kvs_acl, kvs_feed, kvs_subscription]),
    application:set_env(kvs, dba, store_mnesia),
    kvs:join(),
    kvs:init_db(),
    ct:log("-> Dir ~p~n",[kvs:dir()]),
    Config.

end_per_suite(Config) ->
    kvs:destroy(),
    application:stop(kvs),
    ok.

init_per_group(_Name, _Config) ->
    ok.
end_per_group(_Name, _Config) ->
    ok.

access(Config) -> ok.
comment(Config) -> ok.
user(Config) -> ok.
entry(Config) ->
    Fid = 1,
    kvs:add(#entry{id={1,Fid},feed_id=Fid}),
    kvs:add(#entry{id={2,Fid},feed_id=Fid}),
    L = kvs:entries(kvs:get(feed,Fid),entry,undefined),
    List = [ Key || #entry{id=Key} <- L ],
    Length = length(List),
    2 == Length,
    List == [{1,1},{2,1}],
    ct:log("-> List ~p~n", [List]),
    ok.
