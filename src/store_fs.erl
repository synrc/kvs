-module(store_fs).
-copyright('Synrc Research Center s.r.o.').
-include("config.hrl").
-include("metainfo.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

start()    -> ok.
stop()     -> ok.
destroy()  -> ok.
version()  -> {version,"KVS FS"}.
dir()      -> filelib:fold_files("data", "",true, fun(A,Acc)-> [{table,A}|Acc] end, []).
join()     -> ensure_dir("data").
join(Node) -> ok. % should be rsync or smth
change_storage(Table,Type) -> ok.

initialize() ->
    kvs:info(?MODULE,"[store_mnesia] mnesia init.~n",[]),
    mnesia:create_schema([node()]),
    [ kvs:init(store_fs,Module) || Module <- kvs:modules() ],
    mnesia:wait_for_tables([ T#table.name || T <- kvs:tables()],infinity).

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
count(RecordName) -> length(filelib:fold_files(lists:concat(["data/",RecordName]), "",true, fun(A,Acc)-> [A|Acc] end, [])).
all(R) -> filelib:fold_files(lists:concat(["data/",RecordName]), "",true, fun(A,Acc)-> [A|Acc] end, []).
next_id(RecordName, Incr) -> mnesia:dirty_update_counter({id_seq, RecordName}, Incr).
create_table(Name,Options) ->
    X = mnesia:create_table(Name, Options),
    kvs:info("Create table ~p ~nOptions ~p~nReturn ~p~n",[Name, Options,X]),
    X.
add_table_index(Record, Field) -> mnesia:add_table_index(Record, Field).
exec(Q) -> F = fun() -> qlc:e(Q) end, {atomic, Val} = mnesia:transaction(F), Val.
just_one(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, [R]} -> {ok, R};
        {atomic, [_|_]} -> {error, duplicated};
        Error -> Error end.
