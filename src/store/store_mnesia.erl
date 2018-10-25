-module(store_mnesia).
-copyright('Synrc Research Center s.r.o.').
-include("config.hrl").
-include("kvs.hrl").
-include("metainfo.hrl").
-include_lib("mnesia/src/mnesia.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

start()    -> mnesia:start().
stop()     -> mnesia:stop().
destroy()  -> [mnesia:delete_table(T)||{_,T}<-kvs:dir()], mnesia:delete_schema([node()]), ok.
version()  -> {version,"KVS MNESIA"}.
dir()      -> [{table,T}||T<-mnesia:system_info(local_tables)].
join([])   -> mnesia:change_table_copy_type(schema, node(), disc_copies), initialize();
join(Node) ->
    mnesia:change_config(extra_db_nodes, [Node]),
    mnesia:change_table_copy_type(schema, node(), disc_copies),
    [{Tb, mnesia:add_table_copy(Tb, node(), Type)}
     || {Tb, [{N, Type}]} <- [{T, mnesia:table_info(T, where_to_commit)}
                               || T <- mnesia:system_info(tables)], Node==N].

change_storage(Table,Type) -> mnesia:change_table_copy_type(Table, node(), Type).

initialize() ->
    kvs:info(?MODULE,"mnesia init.~n",[]),
    mnesia:create_schema([node()]),
    Res = [ kvs:init(store_mnesia,Module) || Module <- kvs:modules() ],
    mnesia:wait_for_tables([ T#table.name || T <- kvs:tables()],infinity),
    Res.

index(Tab,Key,Value) ->
    Table = kvs:table(Tab),
    Index = string:str(Table#table.fields,[Key]),
    lists:flatten(many(fun() -> mnesia:index_read(Tab,Value,Index+1) end)).

get(RecordName, Key) -> just_one(fun() -> mnesia:read(RecordName, Key) end).
put(Records) when is_list(Records) -> void(fun() -> lists:foreach(fun mnesia:write/1, Records) end);
put(Record) -> put([Record]).
delete(Tab, Key) ->
    case mnesia:activity(context(),fun()-> mnesia:delete({Tab, Key}) end) of
        {aborted,Reason} -> {error,Reason};
        {atomic,_Result} -> ok;
        X -> X end.
count(RecordName) -> mnesia:table_info(RecordName, size).
all(R) -> lists:flatten(many(fun() -> L= mnesia:all_keys(R), [ mnesia:read({R, G}) || G <- L ] end)).
next_id(RecordName, Incr) -> mnesia:dirty_update_counter({id_seq, RecordName}, Incr).
many(Fun) -> case mnesia:activity(context(),Fun) of {atomic, R} -> R; {aborted, Error} -> {error, Error}; X -> X end.
void(Fun) -> case mnesia:activity(context(),Fun) of {atomic, ok} -> ok; {aborted, Error} -> {error, Error}; X -> X end.
info(T) -> try mnesia:table_info(T,all) catch _:_ -> [] end.
create_table(Name,Options) ->
    X = mnesia:create_table(Name, Options),
    kvs:info(?MODULE,"Create table ~p ~nOptions ~p~nReturn ~p~n",[Name, Options,X]),
    X.
add_table_index(Record, Field) -> mnesia:add_table_index(Record, Field).
exec(Q) -> F = fun() -> qlc:e(Q) end, {atomic, Val} = mnesia:activity(context(),F), Val.
just_one(Fun) ->
    case mnesia:activity(context(),Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, [R]} -> {ok, R};
        {atomic, [_|_]} -> {error, duplicated};
        [] -> {error, not_found};
        [R] -> {ok,R};
        [_|_] -> {error, duplicated};
        Error -> Error end.

add(Record) -> mnesia:activity(context(),fun() -> kvs:append(Record,#kvs{mod=?MODULE}) end).
remove(Record,Id) -> mnesia:activity(context(),fun() -> kvs:takeoff(Record,Id,#kvs{mod=?MODULE}) end).
context() -> kvs:config(kvs,mnesia_context,async_dirty).

sync_indexes() ->
    lists:map(fun sync_indexes/1, kvs:tables()).
sync_indexes(#table{name = Table, keys = Keys}) ->
    mnesia:wait_for_tables(Table, 10000),
    #cstruct{attributes = Attrs} = mnesia:table_info(Table, cstruct),
    Indexes = mnesia:table_info(Table, index),
    IndexedKeys = [lists:nth(I-1, Attrs)|| I <- Indexes],
    [mnesia:del_table_index(Table, Key) || Key <- IndexedKeys -- Keys],
    [mnesia:add_table_index(Table, Key) || Key <- Keys -- IndexedKeys].

