-module(kvs_st).
-include("kvs.hrl").
-include("stream.hrl").
-include("metainfo.hrl").
-export(?STREAM).
-import(kvs_rocks, [fmt/1, key/2, key/1, bt/1, tb/1, ref/0, ref/1, seek_it/1, seek_it/2, move_it/3, move_it/4, take_it/4, take_it/5, delete_it/2, estimate/0, estimate/1]).
-export([raw_append/2,raw_append/3, remove/1]).

db() -> application:get_env(kvs,rocks_name,"rocksdb").

se(X,Y,Z) -> setelement(X,Y,Z).
e(X,Y) -> element(X,Y).
c4(R,V) -> se(#reader.args,  R, V).
si(M,T) -> se(#it.id, M, T).
id(T) -> e(#it.id, T).

k(F,[]) -> F;
k(_,{_,Id,SF}) -> iolist_to_binary([SF,<<"/">>,tb(Id)]).

f2(Feed) -> X = tb(Feed),
  case binary:matches(X, <<"/">>,[]) of
    [{0,1}|_] -> binary:part(X,{1,size(X)-1});
            _ -> X
  end.

read_it(C,{ok,_,[],H}) -> C#reader{cache=[], args=lists:reverse(H)};
read_it(C,{ok,F,V,H})  -> C#reader{cache={e(1,V),id(V),F}, args=lists:reverse(H)};
read_it(C,_) -> C#reader{args=[]}.

top(#reader{}=X) -> top(X,db()).
top(#reader{feed=Feed}=C,Db) -> #writer{count=Cn} = writer(f2(Feed),Db), read_it(C#reader{count=Cn},seek_it(Feed,Db)).
bot(#reader{}=X) -> bot(X,db()).
bot(#reader{feed=Feed}=C,Db) -> #writer{cache=Ch, count=Cn} = writer(f2(Feed),Db), C#reader{cache=Ch, count=Cn}.
next(#reader{}=X) -> next(X,db()).
next(#reader{feed=Feed,cache=I}=C,Db) -> read_it(C,move_it(k(Feed,I),Feed,next,Db)).
prev(#reader{}=X) -> prev(X,db()).
prev(#reader{cache=I,feed=Feed}=C,Db) -> read_it(C,move_it(k(Feed,I),Feed,prev,Db)).
take(#reader{}=X) -> take(X,db()).
take(#reader{args=N,feed=Feed,cache=I,dir=1}=C,Db) -> read_it(C,take_it(k(Feed,I),Feed,prev,N,Db));
take(#reader{args=N,feed=Feed,cache=I,dir=_}=C,Db) -> read_it(C,take_it(k(Feed,I),Feed,next,N,Db)).
drop(#reader{}=X) -> drop(X,db()).
drop(#reader{args=N}=C,_) when N =< 0 -> C;
drop(#reader{}=C,Db) -> (take(C#reader{dir=0},Db))#reader{args=[]}.
remove(#reader{}=C) -> remove(C, db()).
remove(#reader{feed=Feed}=C,Db) -> R = read_it(C, delete_it(Feed,Db)), kvs:delete(writer, Feed), R;
remove(Rec,Feed) -> remove(Rec,Feed,db()).

feed(Feed) -> feed(Feed,db()).
feed(Feed,Db) ->
  #reader{count=Cn} = Top = top(reader(Feed,Db),Db),
  Halt = case {estimate(),Cn} of 
          {E,C} when E =< 0 -> max(C,4);
          {E,_} -> E
         end,
  feed(fun(#reader{}=R) -> take(R#reader{args=4},Db) end,Top,[],Halt).

feed(_F,#reader{},Acc,H) when H =< 0 -> Acc;
feed(F,#reader{cache=C1}=R,Acc,H) ->
  #reader{args=A, cache=Ch, feed=Feed} = R1 = F(R),
  case Ch of
    C1 -> Acc ++ A;
    {_,_,K} when binary_part(K,{0,byte_size(Feed)}) == Feed
            andalso length(A) == 4
      -> feed(F, R1, Acc ++ A, H-4);
    _ -> Acc ++ A
  end.

load_reader(Id) -> load_reader(Id,db()).
load_reader(Id,Db) -> case kvs:get(reader,Id,#kvs{db=Db,mod=kvs_rocks}) of {ok,#reader{}=C} -> C; _ -> #reader{id=kvs:seq([],[])} end.

writer(Id) -> writer(Id,db()).
writer(Id,Db) -> case kvs:get(writer,Id,#kvs{db=Db,mod=kvs_rocks}) of {ok,W} -> W; {error,_} -> #writer{id=Id} end.
reader(Id) -> reader(Id,db()).
reader(Id,Db) -> case kvs:get(writer,Id,#kvs{db=Db,mod=kvs_rocks}) of
  {ok,#writer{id=Feed, count=Cn, cache=Ch}} ->
    read_it(#reader{id=kvs:seq([],[]),feed=key(Feed),count=Cn,cache=Ch},seek_it(key(Feed),Db));
  {error,_} ->
    read_it(#reader{id=kvs:seq([],[]),feed=key(Id),count=0,cache=[]},seek_it(key(Id),Db))
  end.
save(C)    -> save(C,db()).
save(C,Db) ->
  N1 = case id(C) of [] -> si(C,kvs:seq([],[])); _ -> C end,
  NC = c4(N1,[]),
  kvs:put(NC,#kvs{db=Db,mod=kvs_rocks}), NC.

% add
raw_append(M,Feed) -> raw_append(M,Feed,db()).
raw_append(M,Feed,Db) -> rocksdb:put(ref(Db), key(Feed,e(2,M)), term_to_binary(M), [{sync,true}]).

add(#writer{}=X) -> add(X, db()).
add(#writer{args=M}=C,Db) when element(2,M) == [] -> add(si(M,kvs:seq([],[])),C,Db);
add(#writer{args=M}=C,Db) -> add(M,C,Db).

add(M,#writer{id=Feed,count=S}=C,Db) ->
   NS=S+1,
   raw_append(M,Feed,Db),
   C#writer{cache={e(1,M),e(2,M),key(Feed)},count=NS}.

cut(Feed) -> cut(Feed,db()).
cut(Feed,Db) ->
  #writer{cache={_,Key,Fd}=Ch} = kvs:writer(Feed, #kvs{db=Db,mod=kvs_rocks}),
  #reader{} = kvs:prev(reader(Feed, Db)),
  #reader{} = kvs:next(#reader{feed=key(Feed), cache=Ch}),

  kvs:delete_range(Feed,{Fd,Key},#kvs{db=Db,mod=kvs_rocks}).

remove(Rec,Feed,Db) ->
  kvs:ensure(#writer{id=Feed},#kvs{db=Db,mod=kvs_rocks}),
  W = #writer{count=C, cache=Ch} = kvs:writer(Feed,#kvs{db=Db,mod=kvs_rocks}),
  Ch1 = case {e(1,Rec),e(2,Rec),key(Feed)} of % need to keep reference for next element
              Ch -> R = reader(Feed,Db), e(4, prev(R#reader{cache=Ch},Db));
              _ -> Ch end,
  case kvs:delete(Feed,id(Rec),#kvs{db=Db,mod=kvs_rocks}) of
        ok -> Count = C - 1,
              save(W#writer{count = Count, cache=Ch1},Db),
              Count;
        _ -> C end.

append(Rec,Feed) -> append(Rec,Feed,db()).
append(Rec,Feed,Db) ->
   kvs:ensure(#writer{id=Feed},#kvs{db=Db,mod=kvs_rocks}),
   Id = e(2,Rec),
   W = writer(Feed,Db),
   case kvs:get(Feed,Id,#kvs{db=Db,mod=kvs_rocks}) of
        {ok,_} -> raw_append(Rec,Feed,Db), Id;
        {error,_} -> save(add(W#writer{args=Rec},Db),Db), Id end.
