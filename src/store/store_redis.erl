-module(store_redis).
-author('Andrey Martemyanov').
-copyright('Synrc Research Center s.r.o.').
-include("config.hrl").
-include("kvs.hrl").
-include("metainfo.hrl").
-compile(export_all).

start() -> erase(eredis_pid), {ok,C}=eredis:start_link(), erlang:put(eredis_pid,C), ok.
stop() -> P=erase(eredis_pid), eredis:stop(P), ok.
c() -> case get(eredis_pid) of
    P when is_pid(P) ->
        case is_process_alive(P) of true -> P; _ -> start(), erlang:get(eredis_pid) end;
    _ -> start(), get(eredis_pid) end.
destroy() -> ok.
version() -> {version,"KVS REDIS"}.
dir() -> [{table,T}||T<-kvs:modules()].
join(_Node) -> initialize(), ok.
change_storage(_Table,_Type) -> ok.
initialize() -> ok.

b2i(B) -> list_to_integer(binary_to_list(B)).
redis_table(RecordName) ->
    list_to_binary(atom_to_list(RecordName)).
redis_key(RecordName,Key) ->
    <<(redis_table(RecordName))/binary,$:,(term_to_binary(Key))/binary>>.
redis_keys(RecordName) ->
    case eredis:q(c(), ["keys", <<(redis_table(RecordName))/binary,$:,$*>> ]) of
        {ok,KeyList} when is_list(KeyList) -> KeyList;
        _ -> [] end.
redis_put(#id_seq{thing=Thing,id=Incr}) when is_integer(Incr)->
    eredis:q(c(), ["SET", redis_key(id_seq,Thing), Incr]);
redis_put(Record) ->
    Key = redis_key(element(1,Record),element(2,Record)),
    Value = term_to_binary(Record),
    eredis:q(c(), ["SET", Key, Value]).
redis_get(Key, Fun) ->
    case eredis:q(c(), ["GET", Key]) of
        {ok, undefined} -> {error,not_found};
        {ok, <<"QUEUED">>} -> transaction;
        {ok, Value} ->
            if is_function(Fun) -> {ok,Fun(Value)};
                true -> {ok,binary_to_term(Value)} end;
        E -> {error, E} end.
redis_get(Key) -> redis_get(Key, undefined).
redis_transaction(Fun) ->
    {ok, <<"OK">>} = eredis:q(c(), ["MULTI"]),
    Fun(),
    {ok,List} = eredis:q(c(), ["EXEC"]),
    List.

index(_RecordName,_Key,_Value) -> not_implemented.
get(id_seq,Key) ->
    redis_get(redis_key(id_seq,Key), fun(Value) ->
        #id_seq{thing=Key,id=b2i(Value)} end);
get(RecordName,Key) -> redis_get(redis_key(RecordName,Key)).
put(Records) when is_list(Records) ->
    redis_transaction(fun() -> lists:foreach(fun put/1, Records) end);
put(Record) -> redis_put(Record).
delete(RecordName,Key) ->
    case eredis:q(c(), ["DEL", redis_key(RecordName,Key)]) of
        {ok,<<"1">>} -> ok;
        E -> {error, E} end.
count(RecordName) -> length(redis_keys(RecordName)).
all(RecordName) ->
    Keys = redis_keys(RecordName),
    List = redis_transaction(fun() -> [redis_get(Key) || Key <- Keys] end),
    case RecordName of
        id_seq ->
            lists:zipwith(fun(K,R) ->
                #id_seq{thing=binary_to_term(binary_part(K,7,size(K)-7)),id=b2i(R)}
                end, Keys, List);
        _ -> [ binary_to_term(R) || R <- List ] end.
next_id(RecordName,Incr) ->
    Key = redis_key(id_seq,RecordName),
    {ok, Value} = eredis:q(c(), ["INCRBY", Key, Incr]),
    b2i(Value).
create_table(_Name,_Options) -> ok.
add_table_index(_Record,_Field) -> not_implemented.
