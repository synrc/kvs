-module(kvs).
-behaviour(application).
-behaviour(supervisor).
-description('KVX Abstract Chain Store').
-include_lib("stdlib/include/assert.hrl").
-include("api.hrl").
-include("metainfo.hrl").
-include("stream.hrl").
-include("cursors.hrl").
-include("kvs.hrl").
-include("backend.hrl").
-export([dump/0,check/0,metainfo/0,ensure/1,seq_gen/0,fold/6,fold/7]).
-export(?API).
-export(?STREAM).
-export([init/1, start/2, stop/1]).

init([]) -> {ok, { {one_for_one, 5, 10}, []} }.
start(_,_) -> supervisor:start_link({local, kvs}, kvs, []).
stop(_) -> ok.

% kvs api

dba()              -> application:get_env(kvs,dba,kvs_mnesia).
kvs_stream()       -> application:get_env(kvs,dba_st,kvs_stream).
all(Table)         -> all     (Table, #kvs{mod=dba()}).
delete(Table,Key)  -> delete  (Table, Key, #kvs{mod=dba()}).
get(Table,Key)     -> ?MODULE:get     (Table, Key, #kvs{mod=dba()}).
index(Table,K,V)   -> index   (Table, K,V, #kvs{mod=dba()}).
join()             -> join    ([],    #kvs{mod=dba()}).
dump()             -> dump    (#kvs{mod=dba()}).
join(Node)         -> join    (Node,  #kvs{mod=dba()}).
leave()            -> leave   (#kvs{mod=dba()}).
count(Table)       -> count   (Table, #kvs{mod=dba()}).
put(Record)        -> ?MODULE:put     (Record, #kvs{mod=dba()}).
fold(Fun,Acc,T,S,C,D) -> fold (Fun,Acc,T,S,C,D, #kvs{mod=dba()}).
stop()             -> stop_kvs(#kvs{mod=dba()}).
start()            -> start   (#kvs{mod=dba()}).
ver()              -> ver(#kvs{mod=dba()}).
dir()              -> dir     (#kvs{mod=dba()}).
seq(Table,DX)      -> seq     (Table, DX, #kvs{mod=dba()}).

% stream api

top  (X) -> (kvs_stream()):top (X).
bot  (X) -> (kvs_stream()):bot (X).
next (X) -> (kvs_stream()):next(X).
prev (X) -> (kvs_stream()):prev(X).
drop (X) -> (kvs_stream()):drop(X).
take (X) -> (kvs_stream()):take(X).
save (X) -> (kvs_stream()):save(X).
cut  (X,Y) -> (kvs_stream()):cut (X,Y).
add  (X) -> (kvs_stream()):add (X).
append  (X, Y) -> (kvs_stream()):append (X, Y).
load_reader (X) -> (kvs_stream()):load_reader(X).
writer      (X) -> (kvs_stream()):writer(X).
reader      (X) -> (kvs_stream()):reader(X).
ensure(#writer{id=Id}) ->
   case kvs:get(writer,Id) of
        {error,_} -> kvs:save(kvs:writer(Id)), ok;
        {ok,_}    -> ok end.

metainfo() ->  #schema { name = kvs, tables = core() }.
core()    -> [ #table { name = id_seq, fields = record_info(fields,id_seq), keys=[thing]} ].

initialize(Backend, Module) ->
    [ begin
        Backend:create_table(T#table.name, [{attributes,T#table.fields},
               {T#table.copy_type, [node()]},{type,T#table.type}]),
        [ Backend:add_table_index(T#table.name, Key) || Key <- T#table.keys ],
        T
    end || T <- (Module:metainfo())#schema.tables ].

all(Tab,#kvs{mod=DBA}) -> DBA:all(Tab).
start(#kvs{mod=DBA}) -> DBA:start().
stop_kvs(#kvs{mod=DBA}) -> DBA:stop().
join(Node,#kvs{mod=DBA}) -> DBA:join(Node).
leave(#kvs{mod=DBA}) -> DBA:leave().
ver(#kvs{mod=DBA}) -> DBA:version().
tables() -> lists:flatten([ (M:metainfo())#schema.tables || M <- modules() ]).
table(Name) when is_atom(Name) -> lists:keyfind(Name,#table.name,tables());
table(_) -> false.
dir(#kvs{mod=DBA}) -> DBA:dir().
modules() -> application:get_env(kvs,schema,[]).
cursors() ->
    lists:flatten([ [ {T#table.name,T#table.fields}
        || #table{name=Name}=T <- (M:metainfo())#schema.tables, Name == reader orelse Name == writer  ]
    || M <- modules() ]).

fold(___,Acc,_,[],_,_,_) -> Acc;
fold(___,Acc,_,undefined,_,_,_) -> Acc;
fold(___,Acc,_,_,0,_,_) -> Acc;
fold(Fun,Acc,Table,Start,Count,Direction,Driver) ->
    try
    case kvs:get(Table, Start, Driver) of
         {ok, R} -> Prev = element(Direction, R),
                    Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
                    fold(Fun, Fun(R,Acc), Table, Prev, Count1, Direction, Driver);
          _Error -> Acc
    end catch _:_ -> Acc end.

seq_gen() ->
    Init = fun(Key) ->
           case kvs:get(id_seq, Key) of
                {error, _} -> {Key,kvs:put(#id_seq{thing = Key, id = 0})};
                {ok, _} -> {Key,skip} end end,
    [ Init(atom_to_list(Name))  || {Name,_Fields} <- cursors() ].


put(Records,#kvs{mod=Mod}) when is_list(Records) -> Mod:put(Records);
put(Record,#kvs{mod=Mod}) -> Mod:put(Record).
get(RecordName, Key, #kvs{mod=Mod}) -> Mod:get(RecordName, Key).
delete(Tab, Key, #kvs{mod=Mod}) -> Mod:delete(Tab, Key).
count(Tab,#kvs{mod=DBA}) -> DBA:count(Tab).
index(Tab, Key, Value,#kvs{mod=DBA}) -> DBA:index(Tab, Key, Value).
seq(Tab, Incr,#kvs{mod=DBA}) -> DBA:seq(Tab, Incr).
dump(#kvs{mod=Mod}) -> Mod:dump().

% tests

check() ->
    Id  = {list,kvs:seq(writer,1)},
    X   = 5,
    _W   = kvs:save(kvs:writer(Id)),
    #reader{id=R1} = kvs:save(kvs:reader(Id)),
    #reader{id=R2} = kvs:save(kvs:reader(Id)),
    [ kvs:save(kvs:add((kvs:writer(Id))#writer{args={emails,[],[],[],[]}})) || _ <- lists:seq(1,X) ],
    Bot = kvs:bot(kvs:load_reader(R1)),
    Top = kvs:top(kvs:load_reader(R2)),
    #reader{args=F} = kvs:take(Bot#reader{args=20,dir=0}),
    #reader{args=B} = kvs:take(Top#reader{args=20,dir=1}),
    ?assertMatch(X,length(F)),
    ?assertMatch(F,lists:reverse(B)).
