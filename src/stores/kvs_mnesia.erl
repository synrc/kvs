-module(kvs_mnesia).
-include("backend.hrl").
-include("kvs.hrl").
-include("metainfo.hrl").
-include_lib("mnesia/src/mnesia.hrl").
-include_lib("stdlib/include/qlc.hrl").
-export(?BACKEND).
-export([info/1,exec/1,dump/1]).

db()       -> "".
seq_pad()  -> application:get_env(kvs, seq_pad, []).
start()    -> mnesia:start().
stop()     -> mnesia:stop().
destroy()  -> [mnesia:delete_table(T)||{_,T}<-kvs:dir()], mnesia:delete_schema([node()]), ok.
destroy(_) -> [mnesia:delete_table(T)||{_,T}<-kvs:dir()], mnesia:delete_schema([node()]), ok.
leave()    -> ok.
leave(_)   -> ok.
version()  -> {version,"KVS MNESIA"}.
dir()      -> [{table,T}||T<-mnesia:system_info(local_tables)].
join([], _)   -> mnesia:start(), mnesia:change_table_copy_type(schema, node(), disc_copies), initialize();

join(Node, _) ->
    mnesia:start(),
    mnesia:change_config(extra_db_nodes, [Node]),
    mnesia:change_table_copy_type(schema, node(), disc_copies),
    [{Tb, mnesia:add_table_copy(Tb, node(), Type)}
     || {Tb, [{N, Type}]} <- [{T, mnesia:table_info(T, where_to_commit)}
                               || T <- mnesia:system_info(tables)], Node==N].

initialize() ->
    mnesia:create_schema([node()]),
    Res = [ kvs:initialize(kvs_mnesia,Module) || Module <- kvs:modules() ],
    mnesia:wait_for_tables([ T#table.name || T <- kvs:tables()],infinity),
    Res.

index(Tab,Key,Value) ->
    lists:flatten(many(fun() -> mnesia:index_read(Tab,Value,Key) end)).

keys(Tab,_) -> mnesia:all_keys(Tab).

key_match(_Tab,_Id,_) -> [].

get(RecordName, Key, _) -> just_one(fun() -> mnesia:read(RecordName, Key) end).
put(R)                  -> put(R,db()).
put(Records, _) when is_list(Records) -> void(fun() -> lists:foreach(fun mnesia:write/1, Records) end);
put(Record, X) -> put([Record], X).
delete(Tab, Key, _) ->
    case mnesia:activity(context(),fun()-> mnesia:delete({Tab, Key}) end) of
        {aborted,Reason} -> {error,Reason};
        {atomic,_Result} -> ok;
        _ -> ok end.
delete_range(_,_,_) -> {error, not_found}.
match(Record) -> lists:flatten(many(fun() -> mnesia:match_object(Record) end)).
index_match(Record, Index) -> lists:flatten(many(fun() -> mnesia:index_match_object(Record, Index) end)).
count(RecordName) -> mnesia:table_info(RecordName, size).
all(R, _) -> lists:flatten(many(fun() -> L= mnesia:all_keys(R), [ mnesia:read({R, G}) || G <- L ] end)).
seq(RecordName, []) -> seq(RecordName, 1);
seq(RecordName, Incr) ->
  Val = integer_to_list(mnesia:dirty_update_counter({id_seq, RecordName}, Incr)),
  case (20 - length(Val) > 0) and lists:member(RecordName, seq_pad()) of true -> string:copies("0", 20 - length(Val)); _ -> "" end ++ Val.

many(Fun) -> case mnesia:activity(context(),Fun) of {atomic, [R]} -> R; {aborted, Error} -> {error, Error}; X -> X end.
void(Fun) -> case mnesia:activity(context(),Fun) of {atomic, ok} -> ok; {aborted, Error} -> {error, Error}; X -> X end.
info(T) -> try mnesia:table_info(T,all) catch _:_ -> [] end.
create_table(Name,Options) -> mnesia:create_table(Name, Options).
add_table_index(Record, Field) -> mnesia:add_table_index(Record, Field).
exec(Q) -> F = fun() -> qlc:e(Q) end, {atomic, Val} = mnesia:activity(context(),F), Val.
just_one(Fun) ->
    case mnesia:activity(context(),Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, [R]} -> {ok, R};
        [] -> {error, not_found};
        [R] -> {ok,R};
        R when is_list(R) -> {ok,R};
        Error -> Error end.

context() -> application:get_env(kvs,mnesia_context,async_dirty).

dump() -> dump([ N || #table{name=N} <- kvs:tables() ]), ok.
dump(short) ->
    Gen = fun(T) ->
        {S,M,C}=lists:unzip3([ dump_info(R) || R <- T ]),
        {lists:usort(S),lists:sum(M),lists:sum(C)}
    end,
    dump_format([ {T,Gen(T)} || T <- [ N || #table{name=N} <- kvs:tables() ] ]);
dump(Table) when is_atom(Table) -> dump(Table);
dump(Tables) ->
    dump_format([{T,dump_info(T)} || T <- lists:flatten(Tables) ]).
dump_info(T) ->
    {mnesia:table_info(T,storage_type),
    mnesia:table_info(T,memory) * erlang:system_info(wordsize) / 1024 / 1024,
    mnesia:table_info(T,size)}.
dump_format(List) ->
    io:format("~20s ~32s ~14s ~10s~n~n",["NAME","STORAGE TYPE","MEMORY (MB)","ELEMENTS"]),
    [ io:format("~20s ~32w ~14.2f ~10b~n",[T,S,M,C]) || {T,{S,M,C}} <- List ],
    io:format("~nSnapshot taken: ~p~n",[calendar:now_to_datetime(os:timestamp())]).
