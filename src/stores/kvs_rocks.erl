-module(kvs_rocks).
-include("backend.hrl").
-include("kvs.hrl").
-include("metainfo.hrl").
-include_lib("stdlib/include/qlc.hrl").
-export(?BACKEND).
-export([ref/0,bt/1,key/2,key/1,fd/1,tb/1]).
-export([seek_it/1, move_it/3, take_it/4]).

e(X,Y) -> element(X,Y).

bt([]) -> [];
bt(X)  -> binary_to_term(X).

tb([]) -> <<>>;
tb(T) when is_list(T) -> unicode:characters_to_nfkc_binary(T);
tb(T) when is_atom(T) -> atom_to_binary(T, utf8);
tb(T) when is_binary(T) -> T;
tb(T) -> term_to_binary(T).
sz([]) -> 0;
sz(B)  -> byte_size(B).

key(R) when is_tuple(R) andalso tuple_size(R) > 1 -> key(e(1,R), e(2,R));
key(R) -> key(R,[]).
key(Tab,R) when is_tuple(R) andalso tuple_size(R) > 1 -> key(Tab, e(2,R));
key(Tab,R) -> Fd = case Tab of [] -> []; _ -> tb(Tab) end,
              iolist_to_binary([lists:join(<<"/">>, lists:flatten([<<>>, Fd, fmt(R)]))]).

fmt([]) -> [];
fmt(K) -> Key = tb(K),
  End = sz(Key),
  {S,E} = case binary:matches(Key, [<<"/">>], []) of
    [{0,1}]         -> {1, End-1};
    [{0,1},{1,1}]   -> {2, End-2};
    [{0,1},{1,1}|_] -> {2, End-2};
    [{0,1}|_]       -> {1, End-1};
    _               -> {0, End}
  end,
  binary:part(Key,{S,E}).

fd(K) -> Key = tb(K),
  End = sz(Key),
  {S,_} = case binary:matches(Key,[<<"/">>],[]) of
    [{0,1}]         -> {End,End};
    [{0,1},{1,1}]   -> {End,End};
    [{0,1},{1,1}|T] -> hd(lists:reverse(T));
    [{0,1}|T]       -> hd(lists:reverse(T));
    _ -> {End,End}
  end,
  binary:part(Key,{0,S}).

run(<<>>,SK,_,_) -> {ok,SK,[],[]};
run(Key, % key
  SK,  % sup-key
  Dir, % direction next/prev
  Compiled_Operations) ->
       % H is iterator reference

  S = sz(SK),
  Initial_Object = {ref(), []},

  Run = fun (F,K,H,V,Acc) when binary_part(K,{0,S}) == SK -> {F(H,Dir),H,[V|Acc]}; % continue +------------+
            (_,K,H,V,Acc) -> stop_it(H),                                           % fail-safe closing     |
                             throw({ok, fd(K), bt(V), [bt(A1) || A1 <- Acc]}) end, % acc unfold            |
                                                                                   %                       |
  Range_Check = fun(F,K,H,V) -> case F(H,prev) of                       % backward prefetch                |
      {ok,K1,V1} when binary_part(K,{0,S}) == SK -> {{ok,K1,V1},H,[V]}; % return (range-check error)       |
      {ok,K1,V1} -> Run(F,K1,H,V1,[]);                                  % run prev-take chain              | loop
      _ -> stop_it(H),                                                  % fail-safe closing                |
           throw({ok, fd(K), bt(V), [bt(V)]})                           % acc unfold                       |
  end end,                                                              %                                  |
                                                                        %                                  |
  State_Machine = fun                                                   %                                  |
    (F,{ok,H})            -> {F(H,{seek,Key}),H};                       % first move (seek)                |
    (F,{{ok,K,V},H}) when Dir =:= prev -> Range_Check(F,K,H,V);         % first chained prev-take          |
    (F,{{ok,K,V},H})      -> Run(F,K,H,V,[]);                           % first chained next-take          |
    (F,{{ok,K,V},H,A})    -> Run(F,K,H,V,A);                            % chained CPS-take continuator +---+
    (_,{{error,E},H,Acc}) -> {{error,E},H,Acc};                         % error effects
    (F,{I,O})             -> F(I,O)                                     % chain constructor from initial object
  end,

  catch case lists:foldl(State_Machine, Initial_Object, Compiled_Operations) of
    {{ok,K,Bin},_,A}  -> {ok, fd(K),  bt(Bin), [bt(A1) || A1 <- A]};
    {{ok,K,Bin},_}    -> {ok, fd(K),  bt(Bin), []};
    {{error,_},_,Acc} -> {ok, fd(SK), bt(shd(Acc)), [bt(A1) || A1 <- Acc]};
    {{error,_},_}     -> {ok, fd(SK), [], []}
  end.

initialize() -> [ kvs:initialize(kvs_rocks,Module) || Module <- kvs:modules() ].
index(_,_,_) -> [].

start()    -> ok.
stop()     -> ok.
destroy()  -> rocksdb:destroy(application:get_env(kvs,rocks_name,"rocksdb"), []).
version()  -> {version,"KVS ROCKSDB"}.
dir()      -> [].
ref()      -> application:get_env(kvs,rocks_ref,[]).
leave()    -> case ref() of [] -> skip; X -> rocksdb:close(X), application:set_env(kvs,rocks_ref,[]), ok end.
join(_)    -> application:start(rocksdb),
              leave(), {ok, Ref} = rocksdb:open(application:get_env(kvs,rocks_name,"rocksdb"), [{create_if_missing, true}]),
              initialize(),
              application:set_env(kvs,rocks_ref,Ref).

compile(seek) -> [fun rocksdb:iterator/2,fun rocksdb:iterator_move/2];
compile(move) -> [fun rocksdb:iterator_move/2];
compile(close) -> [fun rocksdb:iterator_close/1].
compile(take,N) -> lists:map(fun(_) -> fun rocksdb:iterator_move/2 end, lists:seq(1, N)).

stop_it(H) -> try begin [F]=compile(close), F(H) end catch error:badarg -> ok end.
seek_it(K) -> run(K,K,ok,compile(seek)).
move_it(Key,SK,Dir) -> run(Key,SK,Dir,compile(seek) ++ compile(move)).
take_it(Key,SK,Dir,N) when is_integer(N) andalso N >= 0 -> run(Key,SK,Dir,compile(seek) ++ compile(take,N));
take_it(Key,SK,Dir,_) -> take_it(Key,SK,Dir,0).

all(R) -> kvs_st:feed(R).

get(Tab, Key) ->
    case rocksdb:get(ref(), key(Tab,Key), []) of
         not_found -> {error,not_found};
         {ok,Bin} -> {ok,bt(Bin)} end.

put(Records) when is_list(Records) -> lists:map(fun(Record) -> put(Record) end, Records);
put(Record) -> rocksdb:put(ref(), key(Record), term_to_binary(Record), [{sync,true}]).
delete(Feed, Id) -> rocksdb:delete(ref(), key(Feed,Id), []).
count(_) -> 0.

shd([]) -> [];
shd(X) -> hd(X).
create_table(_,_) -> [].
add_table_index(_, _) -> ok.
dump() -> ok.

seq(_,_) ->
  case os:type() of
       {win32,nt} -> {Mega,Sec,Micro} = erlang:timestamp(), integer_to_list((Mega*1000000+Sec)*1000000+Micro);
                _ -> erlang:integer_to_list(element(2,hd(lists:reverse(erlang:system_info(os_monotonic_time_source)))))
  end.
