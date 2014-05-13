-module(store_mnesia).
-copyright('Synrc Research Center s.r.o.').
-include("config.hrl").
-include("metainfo.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

start()    -> mnesia:start().
stop()     -> mnesia:stop().
destroy()  -> [mnesia:delete_table(list_to_atom(T))||{_,T}<-kvs:dir()], mnesia:delete_schema([node()]), ok.
version()  -> {version,"KVS MNESIA"}.
dir()      -> [{table,atom_to_list(T)}||T<-mnesia:system_info(local_tables)].
join()     -> mnesia:change_table_copy_type(schema, node(), disc_copies), initialize().
join(Node) ->
    mnesia:change_config(extra_db_nodes, [Node]),
    mnesia:change_table_copy_type(schema, node(), disc_copies),
    [{Tb, mnesia:add_table_copy(Tb, node(), Type)}
     || {Tb, [{N, Type}]} <- [{T, mnesia:table_info(T, where_to_commit)}
                               || T <- mnesia:system_info(tables)], Node==N].

initialize() ->
    kvs:info(?MODULE,"[store_mnesia] mnesia init.~n"),
    mnesia:create_schema([node()]),
    [ kvs:init(store_mnesia,Module) || Module <- kvs:modules() ],
    mnesia:wait_for_tables([ T#table.name || T <- kvs:tables()],5000).

index(Tab,Key,Value) ->
    Table = kvs:table(Tab),
    Index = string:str(Table#table.fields,[Key]),
    lists:flatten(many(fun() -> mnesia:index_read(Tab,Value,Index+1) end)).

get(RecordName, Key) -> just_one(fun() -> mnesia:read(RecordName, Key) end).
put(Records) when is_list(Records) -> void(fun() -> lists:foreach(fun mnesia:write/1, Records) end);
put(Record) -> put([Record]).
delete(Tab, Key) ->
    case mnesia:transaction(fun()-> mnesia:delete({Tab, Key}) end) of
        {aborted,Reason} -> {error,Reason};
        {atomic,_Result} -> ok end.
count(RecordName) -> mnesia:table_info(RecordName, size).
all(R) -> lists:flatten(many(fun() -> L= mnesia:all_keys(R), [ mnesia:read({R, G}) || G <- L ] end)).
next_id(RecordName, Incr) -> mnesia:dirty_update_counter({id_seq, RecordName}, Incr).
many(Fun) -> case mnesia:transaction(Fun) of {atomic, R} -> R; _ -> [] end.
void(Fun) -> case mnesia:transaction(Fun) of {atomic, ok} -> ok; {aborted, Error} -> {error, Error} end.
create_table(Name,Options) -> mnesia:create_table(Name, Options).
add_table_index(Record, Field) -> mnesia:add_table_index(Record, Field).
exec(Q) -> F = fun() -> qlc:e(Q) end, {atomic, Val} = mnesia:transaction(F), Val.
just_one(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, [R]} -> {ok, R};
        {atomic, [_|_]} -> {error, duplicated};
        Error -> Error end.
