-module(store_mnesia).
-author('Maxim Sokhatsky').
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
-include_lib("kvs/include/log.hrl").
-include_lib("kvs/include/translations.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).
-define(CREATE_TAB(T), create_table(T, record_info(fields, T), [{storage, permanent}]) ).

start() -> mnesia:start(), mnesia:change_table_copy_type(schema, node(), disc_copies).
stop() -> mnesia:stop().
delete() -> mnesia:delete_schema([node()]).
version() -> {version,"KVS MNESIA Embedded"}.
initialize() ->
    mnesia:create_schema([node()]),
    ?CREATE_TAB(payment),
    ?CREATE_TAB(acl),
    ?CREATE_TAB(acl_entry),
    ?CREATE_TAB(feed),
    ?CREATE_TAB(team),
    ?CREATE_TAB(entry),
    ?CREATE_TAB(comment),
    ?CREATE_TAB(user),
    ?CREATE_TAB(user_product),
    ?CREATE_TAB(user_payment),
    ?CREATE_TAB(user_status),
    ?CREATE_TAB(membership),
    ?CREATE_TAB(account),
    ?CREATE_TAB(subscription),
    ?CREATE_TAB(group_subscription),
    ?CREATE_TAB(group),
    ?CREATE_TAB(id_seq),
    ?CREATE_TAB(transaction),
    ?CREATE_TAB(translation),
    ok = add_table_index(comment, entry_id),
    ok = add_table_index(comment, author_id),
    ok = add_table_index(subscription, who),
    ok = add_table_index(subscription, whom),
    ok = add_table_index(group_subscription, who),
    ok = add_table_index(group_subscription, where),
    ok = add_table_index(entry, feed_id),
    ok = add_table_index(entry, entry_id),
    ok = add_table_index(entry, from),
    ok = add_table_index(user, facebook_id),
    ok = add_table_index(user, email),
    ok.

dir() ->
    Tables = mnesia:system_info(local_tables),
    [{table,atom_to_list(T)}||T<-Tables].
get(RecordName, Key) -> just_one(fun() -> mnesia:read(RecordName, Key) end).
put(Records) when is_list(Records) -> void(fun() -> lists:foreach(fun mnesia:write/1, Records) end);
put(Record) -> put([Record]).

delete(Keys) when is_list(Keys) -> void(fun() -> lists:foreach(fun mnesia:delete_object/1, Keys) end);
delete(Keys) -> delete([Keys]).
delete(Tab, Key) -> mnesia:transaction(fun()-> mnesia:delete({Tab, Key}) end), ok.

multi_select(RecordName, Keys) when is_list(Keys) -> flatten(fun() -> [mnesia:read({RecordName, Key}) || Key <- Keys] end).

select(From, PredicateFunction) when is_function(PredicateFunction) -> exec(qlc:q([Record || Record <- mnesia:table(From), apply(PredicateFunction, [Record])]));
select(From, [{where, Fn}, {order, {Idx, Order}}]) -> exec(qlc:q([R || R <- qlc:keysort(Idx, mnesia:table(From), [{order, Order}]), apply(Fn, [R])]));
select(From, [{where, Fn}, {order, {Idx, Order}}, {limit, {1, Length}}]) ->
    {atomic, Recs} = mnesia:transaction(fun()->
        QC = qlc:cursor(qlc:q(
            [R || R <- qlc:keysort(Idx, mnesia:table(From), [{order, Order}]), apply(Fn, [R])])),
        Ret = qlc:eval(qlc:next_answers(QC, Length)),
        qlc:delete_cursor(QC),
        Ret
    end),
    Recs;

select(From, [{where, Fn}, {order, {Idx, Order}}, {limit, {Offset, Length}}]) ->
    {atomic, Recs} = mnesia:transaction(fun()->
        QC = qlc:cursor(qlc:q(
            [R || R <- qlc:keysort(Idx, mnesia:table(From), [{order, Order}]), apply(Fn, [R])])),
        qlc:next_answers(QC, Offset - 1),
        Ret = qlc:eval(qlc:next_answers(QC, Length)),
        qlc:delete_cursor(QC),
        Ret
    end),
    Recs;

select(RecordName, Key) -> many(fun() -> mnesia:read({RecordName, Key}) end).
count(RecordName) -> mnesia:table_info(RecordName, size).
all(RecordName) -> flatten(fun() -> Lists = mnesia:all_keys(RecordName), [ mnesia:read({RecordName, G}) || G <- Lists ] end).
all_by_index(RecordName,Key,Value) -> flatten(fun() -> mnesia:index_read(RecordName,Value,Key) end).

next_id(RecordName) -> next_id(RecordName, 1).
next_id(RecordName, Incr) -> mnesia:dirty_update_counter({id_seq, RecordName}, Incr).

just_one(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, [R]} -> {ok, R};
        {atomic, [_|_]} -> {error, duplicated};
        _ -> {error, not_found} end.

flatten(Fun) -> case mnesia:transaction(Fun) of {atomic, R} -> lists:flatten(R); _ -> [] end.
many(Fun) -> case mnesia:transaction(Fun) of {atomic, R} -> R; _ -> [] end.
void(Fun) -> case mnesia:transaction(Fun) of {atomic, ok} -> ok; {aborted, Error} -> {error, Error} end.

create_table(Record, RecordInfo,  Opts0) ->
    Attr = [{attributes, RecordInfo}],
    Opts = transform_opts(Opts0),
    AllOpts = lists:concat([Opts, Attr]),
    case mnesia:create_table(Record, lists:flatten(AllOpts)) of
        {atomic, ok}                         -> ok;
        {aborted, {already_exists, Record}}  -> ok;
        {aborted, Err}                       -> {error, Err} end.

add_table_index(Record, Field) ->
    case mnesia:add_table_index(Record, Field) of
        {atomic, ok}                        -> ok;
        {aborted,{already_exists,Record,_}} -> ok;
        {aborted, Err}                      -> {error, Err} end.

transform_opts(Opts) -> transform_opts(Opts, []).
transform_opts([], Acc) -> lists:reverse(Acc);
transform_opts([{storage, Value} | Rest], Acc0) ->
    NewOpts = storage_to_mnesia_type(Value),
    Acc = [NewOpts | Acc0],
    transform_opts(Rest, Acc);
transform_opts([Other | Rest], Acc0) ->
    Acc = [Other | Acc0],
    transform_opts(Rest, Acc).

storage_to_mnesia_type(permanent) -> {disc_copies, [node()]};
storage_to_mnesia_type(temporary) -> {ram_copies, [node()]};
storage_to_mnesia_type(ondisk) -> {disc_only_copies, [node()]}.

exec(Q) -> F = fun() -> qlc:e(Q) end, {atomic, Val} = mnesia:transaction(F), Val.

% index funs

products(UId) -> all_by_index(user_product, #user_product.username, UId).
subscriptions(UId) -> all_by_index(subsciption, #subscription.who, UId).
subscribed(Who) -> all_by_index(subscription, #subscription.whom, Who).
participate(UserName) -> all_by_index(group_subscription, #group_subscription.who, UserName).
members(GroupName) -> all_by_index(group_subscription, #group_subscription.where, GroupName).
user_tournaments(UId) -> all_by_index(play_record, #play_record.who, UId).
tournament_users(TId) -> all_by_index(play_record, #play_record.tournament, TId).
author_comments(Who) ->
    EIDs = [E || #comment{entry_id=E} <- all_by_index(comment,#comment.author_id, Who) ],
    lists:flatten([ all_by_index(entry, #entry.id,EID) || EID <- EIDs]).
