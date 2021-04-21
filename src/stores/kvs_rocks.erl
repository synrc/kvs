-module(kvs_rocks).
-include("backend.hrl").
-include("kvs.hrl").
-include("metainfo.hrl").
-include_lib("stdlib/include/qlc.hrl").
-export(?BACKEND).
-export([ref/0,bt/1,key/2,key/1,fd/1, tb/1]).
-export([seek_it/1, move_it/3, take_it/4]).

e(X,Y)     -> element(X,Y).
bt([])     -> [];
bt(X)      -> binary_to_term(X).
tb([])     -> [];
tb(T) when is_list(T) -> list_to_binary(T);
tb(T) when is_atom(T) -> atom_to_binary(T,utf8);
tb(T) when is_binary(T) -> T;
tb(T)      -> term_to_binary(T).

fmt([]) -> [];
fmt(K) ->
  B = tb(K),
  S = if byte_size(B) > 1 -> 2;true -> byte_size(B) end,
  B1 = binary:split(B,[<<"/">>,<<"//">>], [{scope,{0,S}},trim_all]),
  B2 = case B1 of [] -> <<>>;[X|_] -> X end,
  S1 = if byte_size(B2) > 1 -> -2; true -> 0 end,
  B3 = binary:split(B2,[<<"/">>,<<"//">>], [{scope,{byte_size(B2),S1}},trim_all]),
  B4 = case B3 of [] -> <<>>;[X1|_] -> X1 end,
  B4.

key(R)     when is_tuple(R) andalso tuple_size(R) > 1 -> key(e(1,R), e(2,R));
key(R)     -> key(R,[]).
key(Tab,R) when is_tuple(R) andalso tuple_size(R) > 1 -> key(Tab, e(2,R));
key(Tab,R) ->
  iolist_to_binary([lists:join(<<"/">>, lists:flatten([<<>>, tb(Tab), fmt(R)]))]).

fd(K) -> Key = tb(K),
  End = byte_size(Key),
  {S,_} = case binary:matches(Key,[<<"/">>],[]) of
    [{0,1}]         -> {End,End};
    [{0,1},{1,1}]   -> {End,End};
    [{0,1},{1,1}|T] -> hd(lists:reverse(T));
    [{0,1}|T]       -> hd(lists:reverse(T));
    _ -> {End,End}
  end,
  binary:part(Key,{0,S}).

o(<<>>,FK,_,_) -> {ok,FK,[],[]};
o(Key,FK,Dir,Fx) ->
  S = size(FK),

  Infotech = fun (F,K,H,V,Acc) when binary_part(K,{0,S}) == FK -> {F(H,Dir),H,[V|Acc]};
                 (_,K,H,V,Acc) -> close_it(H),
                                  throw({ok,fd(K),bt(V),[bt(A1)||A1<-Acc]}) end,
  Privat = fun(F,K,V,H) -> case F(H,prev) of
      {ok,K1,V1} when binary_part(K,{0,S}) == FK -> {{ok,K1,V1},H,[V]};
      {ok,K1,V1} -> Infotech(F,K1,H,V1,[]);
      E -> E
  end end,

  It = fun(F,{ok,H})            -> {F(H,{seek,Key}),H};
          (F,{{ok,K,V},H})
              when Dir =:= prev -> Privat(F,K,V,H);
          (F,{{ok,K,V},H})      -> Infotech(F,K,H,V,[]);
          (F,{{ok,K,V},H,A})    -> Infotech(F,K,H,V,A);
          (_,{{error,_},H,Acc}) -> {{ok,[],[]},H,Acc};
          (F,{R,O})             -> F(R,O);
          (F,H)                 -> F(H) end,
  catch case lists:foldl(It, {ref(),[]}, Fx) of
    {{ok,K,Bin},_,A}  -> {ok,fd(K), bt(Bin),[bt(A1)||A1<-A]};
    {{ok,K,Bin},_}    -> {ok,fd(K), bt(Bin),[]};
    {{error,_},_,Acc} -> {ok,fd(FK),bt(shd(Acc)),[bt(A1) ||A1<-Acc]}
  end.

start()    -> ok.
stop()     -> ok.
destroy()  -> rocksdb:destroy(application:get_env(kvs,rocks_name,"rocksdb"), []).
version()  -> {version,"KVS ROCKSDB"}.
dir()      -> [].
leave() -> case ref() of [] -> skip; X -> rocksdb:close(X), application:set_env(kvs,rocks_ref,[]), ok end.
join(_) -> application:start(rocksdb),
           leave(), {ok, Ref} = rocksdb:open(application:get_env(kvs,rocks_name,"rocksdb"), [{create_if_missing, true}]),
           initialize(),
           application:set_env(kvs,rocks_ref,Ref).
initialize() -> [ kvs:initialize(kvs_rocks,Module) || Module <- kvs:modules() ].
ref() -> application:get_env(kvs,rocks_ref,[]).
index(_,_,_) -> [].

close_it(H) -> try rocksdb:iterator_close(H) catch error:badarg -> ok end.
seek_it(K) -> o(K,K,ok,[fun rocksdb:iterator/2,fun rocksdb:iterator_move/2]).
move_it(K,FK,Dir) -> o(K,FK,Dir,[fun rocksdb:iterator/2,fun rocksdb:iterator_move/2,fun rocksdb:iterator_move/2]).
take_it(Key,FK,Dir,N) when is_integer(N) andalso N >= 0 ->
  o(Key,FK,Dir,[fun rocksdb:iterator/2,fun rocksdb:iterator_move/2] ++
               lists:map(fun(_) -> fun rocksdb:iterator_move/2 end,lists:seq(1,N)));
take_it(Key,FK,Dir,_) -> take_it(Key,FK,Dir,0).

get(Tab, Key) ->
    case rocksdb:get(ref(), key(Tab,Key), []) of
         not_found -> {error,not_found};
         {ok,Bin} -> {ok,bt(Bin)} end.

put(Records) when is_list(Records) -> lists:map(fun(Record) -> put(Record) end, Records);
put(Record) -> rocksdb:put(ref(), key(Record), term_to_binary(Record), [{sync,true}]).
delete(Feed, Id) -> rocksdb:delete(ref(), key(Feed,Id), []).

count(_) -> 0.

all(R) -> kvs_st:feed(R).

shd([]) -> [];
shd(X) -> hd(X).

seq(_,_) ->
  case os:type() of
       {win32,nt} -> {Mega,Sec,Micro} = erlang:timestamp(), integer_to_list((Mega*1000000+Sec)*1000000+Micro);
                _ -> erlang:integer_to_list(element(2,hd(lists:reverse(erlang:system_info(os_monotonic_time_source)))))
  end.

create_table(_,_) -> [].
add_table_index(_, _) -> ok.
dump() -> ok.
