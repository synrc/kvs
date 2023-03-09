-module(kvs_rocks).
-include("backend.hrl").
-include("kvs.hrl").
-include("metainfo.hrl").
-include_lib("stdlib/include/qlc.hrl").
-export(?BACKEND).
-export([ref/0,ref/1,bt/1,key/2,key/1,fd/1,tb/1,estimate/0,estimate/1]).
-export([seek_it/1, seek_it/2, move_it/3, move_it/4, take_it/4, take_it/5, delete_it/1, delete_it/2]).

e(X,Y) -> element(X,Y).

bt([]) -> [];
bt(X)  -> binary_to_term(X).

tb([]) -> <<>>;
tb(T) when is_list(T) -> unicode:characters_to_nfkc_binary(T);
tb(T) when is_atom(T) -> erlang:atom_to_binary(T, utf8);
tb(T) when is_binary(T) -> T;
tb(T) -> term_to_binary(T).
sz([]) -> 0;
sz(B)  -> byte_size(B).

key(R) when is_tuple(R) andalso tuple_size(R) > 1 -> key(e(1,R), e(2,R));
key(R) -> key(R,[]).
key(writer,R) -> % allow old writers
              iolist_to_binary([lists:join(<<"/">>, lists:flatten([<<>>, erlang:atom_to_binary(writer, utf8), tb(R)]))]);
key(Tab,R) -> Fd = case Tab of [] -> []; _ -> tb(Tab) end,
              iolist_to_binary([lists:join(<<"/">>, lists:flatten([<<>>, Fd, fmt(R)]))]).
keys(Tab, Db) ->
    Feed = key(Tab,[]),
    {ok, H} = rocksdb:iterator(ref(Db), []),
    Keys = fun KEY(K1,Acc) when binary_part(K1,{0,byte_size(Feed)}) =:= Feed ->
                  case rocksdb:iterator_move(H, next) of
                    {ok,K2,_} -> KEY(K2,[tb(K1)|Acc]);
                            _ -> lists:reverse([tb(K1)|Acc])
                  end;
               KEY(_,Acc) -> rocksdb:iterator_close(H), lists:reverse(Acc)
           end,
    {ok, K, _} = rocksdb:iterator_move(H, {seek, Feed}),
    Keys(K,[]).

 key_match(Tab, Id, Db) ->
  Feed = key(Tab,[]),
  {ok, H} = rocksdb:iterator(ref(Db), []),
  Keys = fun KEY(K1) when 
              binary_part(K1,{0,byte_size(Feed)}) =:= Feed andalso
              binary_part(K1,{byte_size(K1), -byte_size(Id)}) =:= Id ->
              rocksdb:iterator_close(H), [K1];
             KEY(K1) when binary_part(K1,{0,byte_size(Feed)}) =:= Feed ->
                case rocksdb:iterator_move(H, next) of
                  {ok,K2,_} -> KEY(K2);
                          _ -> []
                end;
             KEY(_) -> rocksdb:iterator_close(H), []
         end,
  {ok, K, _} = rocksdb:iterator_move(H, {seek, Feed}),
  Keys(K).

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

run(<<>>,SK,_,_,_) -> {ok,SK,[],[]};
run(Key, % key
  SK,  % sup-key
  Dir, % direction next/prev
  Compiled_Operations,
  Db) ->
       % H is iterator reference

  S = sz(SK),
  Initial_Object = {ref(Db), []},

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
    {{ok,K,Bin},H,A}  -> stop_it(H), {ok, fd(K),  bt(Bin), [bt(A1) || A1 <- A]};
    {{ok,K,Bin},H}    -> stop_it(H), {ok, fd(K),  bt(Bin), []};
    {{error,_},H,Acc} -> stop_it(H), {ok, fd(SK), bt(shd(Acc)), [bt(A1) || A1 <- Acc]};
    {{error,_},H}     -> stop_it(H), {ok, fd(SK), [], []}
  end.

initialize() -> [ kvs:initialize(kvs_rocks,Module) || Module <- kvs:modules() ].
index(_,_,_) -> [].

ref_env(Db)      -> list_to_atom("rocks_ref_" ++ Db).
db()             -> application:get_env(kvs,rocks_name,"rocksdb").
start()          -> ok.
stop()           -> ok.
destroy()        -> destroy(db()).
destroy(Db)      -> rocksdb:destroy(Db, []).
version()        -> {version,"KVS ROCKSDB"}.
dir()            -> [].
match(_)         -> [].
index_match(_,_) -> [].
ref()            -> ref(db()).
ref(Db)          -> application:get_env(kvs,ref_env(Db),[]).
leave()          -> leave(db()).
leave(Db)        -> case ref(Db) of [] -> skip; X -> rocksdb:close(X), application:set_env(kvs,ref_env(Db),[]), ok end.
join(_,Db)       ->
              application:start(rocksdb),
              leave(Db), {ok, Ref} = rocksdb:open(Db, [{create_if_missing, true}]),
              initialize(),
              application:set_env(kvs,ref_env(Db),Ref).

compile(it)     -> [fun rocksdb:iterator/2];
compile(seek)   -> [fun rocksdb:iterator/2,fun rocksdb:iterator_move/2];
compile(move)   -> [fun rocksdb:iterator_move/2];
compile(close)  -> [fun rocksdb:iterator_close/1].
compile(take,N) -> lists:map(fun(_) -> fun rocksdb:iterator_move/2 end, lists:seq(1, N)).
compile(delete,_, {error,E},_) -> {error,E};
compile(delete,SK,{ok,_,V1,_},Db) ->
  F1 = key(key(fmt(SK),e(2,V1))), S = sz(SK),
  [fun Del(H,Dir) ->
    case rocksdb:delete(ref(Db), F1, []) of ok ->      
      % {ok, K} case exist only in api, but never actually used
      case rocksdb:iterator_move(H,Dir) of
        {ok,K,_} when binary_part(K,{0,S}) == SK -> case rocksdb:delete(ref(Db), K, []) of ok -> Del(H,Dir); E -> E end;
        {ok,K}   when binary_part(K,{0,S}) == SK -> case rocksdb:delete(ref(Db), K, []) of ok -> Del(H,Dir); E -> E end;
        {ok,K,V} -> {ok,K,V};
        {ok,K}   -> {ok, K};
        E -> E
      end;
      E -> E
    end
  end].

stop_it(H) -> try begin [F]=compile(close), F(H) end catch error:badarg -> ok end.
seek_it(K) -> seek_it(K,db()).
seek_it(K,Db) -> run(K,K,ok,compile(seek),Db).
move_it(Key,SK,Dir) -> move_it(Key,SK,Dir,db()).
move_it(Key,SK,Dir,Db) -> run(Key,SK,Dir,compile(seek) ++ compile(move),Db).
take_it(Key,SK,Dir,N) -> take_it(Key,SK,Dir,N,db()).
take_it(Key,SK,Dir,N,Db) when is_integer(N) andalso N >= 0 -> run(Key,SK,Dir,compile(seek) ++ compile(take,N),Db);
take_it(Key,SK,Dir,_,Db) -> take_it(Key,SK,Dir,0,Db).
delete_it(Fd) -> delete_it(Fd, db()).
delete_it(Fd,Db) -> run(Fd,Fd,next,compile(seek) ++ compile(delete,Fd,seek_it(Fd),Db),Db).

all(R,Db) -> kvs_st:feed(R,Db).

get(Tab, {step,N,[208|_]=Key}, Db) -> get(Tab, {step,N,list_to_binary(Key)},Db);
get(Tab, [208|_]=Key, Db) -> get(Tab, list_to_binary(Key), Db);
get(Tab, Key, Db) ->
    case rocksdb:get(ref(Db), key(Tab,Key), []) of
         not_found -> {error,not_found};
         {ok,Bin} -> {ok,bt(Bin)} end.

put(Record) -> put(Record,db()).
put(Records,Db) when is_list(Records) -> lists:map(fun(Record) -> put(Record,Db) end, Records);
put(Record,Db) -> rocksdb:put(ref(Db), key(Record), term_to_binary(Record), [{sync,true}]).
delete(Feed, Id, Db) -> rocksdb:delete(ref(Db), key(Feed,Id), []).
delete_range(Feed,{Fd,Key},Db) ->
  Last = key(key(fmt(Fd),Key)),
  ReadOps = [{'prefix_same_as_start', true}],
  CompactOps = [{change_level, true}],
  Feed1 = key(Feed),
  Sz = size(Feed1),
  Reopen = case ref(Db) of [] -> skip; _ -> leave(Db), ok end,

  {ok, R} = rocksdb:open(Db, [{prefix_extractor, {capped_prefix_transform, Sz}}]),
  {ok, H} = rocksdb:iterator(R, ReadOps),
  {ok, Start, _} = rocksdb:iterator_move(H, {seek, Feed1}),

  ok = rocksdb:delete_range(R, Start, Last, []),
  ok = rocksdb:delete(R, Last, []),
  ok = rocksdb:delete(R, key(writer,Feed), []),
  ok = rocksdb:compact_range(R, Start, undefined, CompactOps),
  ok = rocksdb:iterator_close(H),
  ok = rocksdb:close(R),
  case Reopen of skip -> ok; ok -> join([],Db) end.
count(_) -> 0.
estimate()   -> estimate(db()).
estimate(Db) -> case rocksdb:get_property(ref(Db), <<"rocksdb.estimate-num-keys">>) of
                {ok, Est} when is_binary(Est)  -> binary_to_integer(Est);
                {ok, Est} when is_list(Est)    -> list_to_integer(Est);
                {ok, Est} when is_integer(Est) -> Est;
                _ -> 0 
            end.

shd([]) -> [];
shd(X) -> hd(X).
create_table(_,_) -> [].
add_table_index(_, _) -> ok.
dump() -> ok.

seq(_,_) ->
  Val =
    case os:type() of
      {win32,nt} -> {Mega,Sec,Micro} = erlang:timestamp(), integer_to_list((Mega*1000000+Sec)*1000000+Micro);
      _ -> integer_to_list(element(2,hd(lists:reverse(erlang:system_info(os_system_time_source)))))
    end,
  case 20 - length(Val) > 0 of true -> string:copies("0", 20 - length(Val)); _ -> "" end ++ Val.
