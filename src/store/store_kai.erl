-module(store_kai).
-author('Maxim Sokhatsky').
-copyright('Synrc Research Center s.r.o.').
-include("config.hrl").
-include("metainfo.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).
-record(data, { key, bucket, last_modified, vector_clocks, checksum, flags, value }).

start() -> kai:start(), ok.
stop() -> kai_store:stop(), ok.
version() -> {version,"KVS KAI"}.
join(_Node) -> initialize(), ok.
initialize() -> ok.
dir() -> [{table,T}||T<-kvs:modules()].

put(Records) when is_list(Records) -> lists:foreach(fun kai_put/1, Records);
put(Record) -> kai_put(Record).

kai_put(Record) ->
    Data = #data{key = element(2,Record), bucket = table_to_num(element(1,Record)),
        last_modified = os:timestamp(), checksum = erlang:md5(term_to_binary(Record)),
        vector_clocks = vclock:fresh(), value = Record },
    kai_store:put(Data).

update(_Record, _Object) -> ok.

get(Tab, Key) ->
    Data = #data{key=Key,bucket=table_to_num(Tab)},
    kai_get(Data).

kai_get(Data) ->
    case kai_store:get(Data) of
         #data{value=Value} -> {ok,Value};
         undefined -> {error,not_found};
         E -> {error,E} end.

delete(Tab, Key) ->
    Data = #data{key=Key,bucket=table_to_num(Tab)},
    kai_delete(Data).

kai_delete(Data) ->
    case kai_store:delete(Data) of
         ok -> ok;
         E -> {error,E} end.

key_to_bin(Key) ->
    if is_integer(Key) -> erlang:list_to_binary(integer_to_list(Key));
       is_list(Key) -> erlang:list_to_binary(Key);
       is_atom(Key) -> erlang:list_to_binary(erlang:atom_to_list(Key));
       is_binary(Key) -> Key;
       true ->  [ListKey] = io_lib:format("~p", [Key]), erlang:list_to_binary(ListKey) end.

all(RecordName) ->
    {list_of_data,List} = kai_store:list(table_to_num(RecordName)),
    [ begin {ok,Val}=kai_get(Data),Val end || Data <- List ].

all_by_index(_Tab, _IndexId, _IndexVal) -> [].

table_to_num(user)         -> 10;
table_to_num(subscription) -> 11;
table_to_num(group)        -> 12;
table_to_num(id_seq)       -> 13;
table_to_num(product)      -> 14;
table_to_num(acl)          -> 15;
table_to_num(feed)         -> 16;
table_to_num(entry)        -> 17;
table_to_num(comment)      -> 18;
table_to_num(Name) -> case kvs:config(kvs,table_to_num) of
                        [] -> unknown_table;
                        Module -> Module:table_to_num(Name) end.
