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
dir()      -> [ {table,F} || F <- filelib:wildcard("data/*"), filelib:is_dir(F) ].
join()     -> filelib:ensure_dir("data/").
join(Node) -> ok. % should be rsync or smth
change_storage(Table,Type) -> ok.

initialize() ->
    kvs:info(?MODULE,"[store_mnesia] mnesia init.~n",[]),
    mnesia:create_schema([node()]),
    [ kvs:init(store_fs,Module) || Module <- kvs:modules() ],
    mnesia:wait_for_tables([ T#table.name || T <- kvs:tables()],infinity).

index(Tab,Key,Value) -> ok.
get(TableName, Key) ->
    HashKey = wf:url_encode(base64:encode(crypto:sha(term_to_binary(Key)))),
    Dir = lists:concat(["data/",TableName,"/"]),
    case file:read_file(lists:concat([Dir,HashKey])) of
         {ok,Binary} -> {ok,binary_to_term(Binary,[safe])};
         {error,Reason} -> {error,Reason} end.

put(Records) when is_list(Records) -> lists:map(fun(Record) -> put(Record) end, Records);
put(Record) ->
    TableName = element(1,Record),
    HashKey = wf:url_encode(base64:encode(crypto:sha(term_to_binary(element(2,Record))))),
    BinaryValue = term_to_binary(Record),
    Dir = lists:concat(["data/",TableName,"/"]),
    filelib:ensure_dir(Dir),
    File = lists:concat([Dir,HashKey]),
    io:format("File: ~p~n",[File]),
    file:write_file(File,BinaryValue,[write,raw,binary,exclusive,sync]).

delete(Tab, Key) -> ok.
count(RecordName) -> length(filelib:fold_files(lists:concat(["data/",RecordName]), "",true, fun(A,Acc)-> [A|Acc] end, [])).
all(R) -> filelib:fold_files(lists:concat(["data/",R]), "",true, fun(A,Acc)-> [A|Acc] end, []).
next_id(RecordName, Incr) -> mnesia:dirty_update_counter({id_seq, RecordName}, Incr).
create_table(Name,Options) -> filelib:ensure_dir(lists:concat(["data/",Name,"/"])).
add_table_index(Record, Field) -> ok.
