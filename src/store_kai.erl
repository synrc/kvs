-module(store_kai).
-author('Maxim Sokhatsky <maxim@synrc.com>').
-copyright('Synrc Research Center s.r.o.').
-include_lib("kvs/include/config.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/acls.hrl").
-include_lib("kvs/include/invites.hrl").
-include_lib("kvs/include/meetings.hrl").
-include_lib("kvs/include/membership.hrl").
-include_lib("kvs/include/payments.hrl").
-include_lib("kvs/include/purchases.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

start() -> ok.
stop() -> ok.
version() -> {version,"KVS KAI PURE XEN"}.
join() -> initialize(), ok.
join(Node) -> initialize(), ok.

initialize() -> ok.

dir() -> ok.

put(Records) when is_list(Records) -> lists:foreach(fun kai_put/1, Records);
put(Record) -> kai_put(Record).

kai_put(Record) -> ok.

update(Record, Object) -> ok.

get(Tab, Key) ->
    ok.

delete(Tab, Key) ->
    ok.

key_to_bin(Key) ->
    if is_integer(Key) -> erlang:list_to_binary(integer_to_list(Key));
       is_list(Key) -> erlang:list_to_binary(Key);
       is_atom(Key) -> erlang:list_to_binary(erlang:atom_to_list(Key));
       is_binary(Key) -> Key;
       true ->  [ListKey] = io_lib:format("~p", [Key]), erlang:list_to_binary(ListKey) end.

all(RecordName) ->
    [].

all_by_index(Tab, IndexId, IndexVal) -> [].


% index funs

products(UId) -> all_by_index(user_product, <<"user_bin">>, list_to_binary(UId)).
subscriptions(UId) -> all_by_index(subsciption, <<"subs_who_bin">>, list_to_binary(UId)).
subscribed(Who) -> all_by_index(subscription, <<"subs_whom_bin">>, list_to_binary(Who)).
participate(UserName) -> all_by_index(group_subscription, <<"who_bin">>, UserName).
members(GroupName) -> all_by_index(group_subscription, <<"where_bin">>, GroupName).
user_tournaments(UId) -> all_by_index(play_record, <<"play_record_who_bin">>, list_to_binary(UId)).
tournament_users(TId) -> all_by_index(play_record, <<"play_record_tournament_bin">>, list_to_binary(integer_to_list(TId))).
author_comments(Who) ->
    EIDs = [E || #comment{entry_id=E} <- all_by_index(comment,<<"author_bin">>, Who) ],
    lists:flatten([ all_by_index(entry,<<"entry_bin">>,EID) || EID <- EIDs]).
