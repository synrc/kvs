-module(kvs_st).
-description('KVS STREAM NATIVE ROCKS').
-include("kvs.hrl").
-include("stream.hrl").
-include("metainfo.hrl").
-export(?STREAM).
-export([prev/8,ref/0,feed_key/2]).

bt(X) -> kvs_rocks:bt(X).
ref() -> kvs_rocks:ref().

% section: kvs_stream prelude

se(X,Y,Z)  -> setelement(X,Y,Z).
e(X,Y)  -> element(X,Y).
c4(R,V) -> se(#reader.args,  R, V).
si(M,T) -> se(#it.id, M, T).
id(T)   -> e(#it.id, T).

% section: next, prev

top  (#reader{}=C) -> C.
bot  (#reader{}=C) -> C.

next (#reader{cache=[]}) -> {error,empty};
next (#reader{feed=Feed,cache=I}=C) when is_tuple(I) ->
   Key = feed_key(I,Feed),
   rocksdb:iterator_move(I, {seek,Key}),
   case rocksdb:iterator_move(I, next) of
        {ok,_,Bin} -> C#reader{cache=bt(Bin)};
            {error,Reason} -> {error,Reason} end.

prev (#reader{cache=[]}) -> {error,empty};
prev (#reader{cache=I,id=Feed}=C) when is_tuple(I) ->
   Key = feed_key(I,Feed),
   rocksdb:iterator_move(I, {seek,Key}),
   case rocksdb:iterator_move(I, prev) of
        {ok,_,Bin} -> C#reader{cache=bt(Bin)};
            {error,Reason} -> {error,Reason} end.

% section: take, drop

drop(#reader{args=N}) when N < 0 -> #reader{};

drop(#reader{args=N,feed=Feed,cache=I}=C) when N == 0 ->
   Key = list_to_binary(lists:concat(["/",kvs_rocks:format(Feed)])),
   case rocksdb:iterator_move(I, {seek,Key}) of
        {ok,_,Bin} -> C#reader{cache=bt(Bin)};
                 _ -> C#reader{cache=[]} end;

drop(#reader{args=N,feed=Feed,cache=I}=C) when N > 0 ->
   Key   = list_to_binary(lists:concat(["/",kvs_rocks:format(Feed)])),
   First = rocksdb:iterator_move(I, {seek,Key}),
   Term  = lists:foldl(
    fun (_,{{ok,K,_},{_,X}}) when N > X -> {K,{<<131,106>>,N}};
        (_,{{ok,K,Bin},{A,X}}) when N =< X->
           case binary:part(K,0,size(Key)) of
                Key -> {rocksdb:iterator_move(I,next),{Bin,X+1}};
                  _ -> {{error,range},{A,X}} end;
        (_,{_,{_,_}}) -> {[],{<<131,106>>,N}}
     end,
           {First,{<<131,106>>,1}},
           lists:seq(0,N)),
   C#reader{cache=bt(element(1,element(2,Term)))}.

take(#reader{args=N,feed=Feed,cache={T,O}}=C) ->
   Key = list_to_binary(lists:concat(["/",kvs_rocks:format(Feed)])),
   {ok,I} = rocksdb:iterator(ref(), []),
   {ok,K,BERT} = rocksdb:iterator_move(I, {seek,feed_key({T,O},Feed)}),
   Res = kvs_rocks:next(I,Key,size(Key),K,BERT,[],case N of -1 -> -1; J -> J + 1 end,0),
   case {Res,length(Res) < N + 1 orelse N == -1} of
        {[],_}    -> C#reader{args=[],cache=[]};
        {[H|X],false} -> C#reader{args=X,cache={e(1,H),e(2,H)}};
        {[H|X],true} -> C#reader{args=Res,cache=[]} end.

% new, save, load, up, down, top, bot

load_reader (Id) ->
    case kvs:get(reader,Id) of
         {ok,#reader{}=C} -> C;
              _ -> #reader{id=[]} end.

writer (Id) -> case kvs:get(writer,Id) of {ok,W} -> W; {error,_} -> #writer{id=Id} end.
reader (Id) ->
    case kvs:get(writer,Id) of
         {ok,#writer{id=Feed}} ->
             Key = list_to_binary(lists:concat(["/",kvs_rocks:format(Feed)])),
             {ok,I} = rocksdb:iterator(ref(), []),
             {ok,K,BERT} = rocksdb:iterator_move(I, {seek,Key}),
             F = bt(BERT),
             #reader{id=kvs:seq([],[]),feed=Id,cache={e(1,F),e(2,F)}};
         {error,_} -> #reader{} end.
save (C) -> NC = c4(C,[]), kvs:put(NC), NC.

% add

add(#writer{args=M}=C) when element(2,M) == [] -> add(si(M,kvs:seq([],[])),C);
add(#writer{args=M}=C) -> add(M,C).

add(M,#writer{id=Feed,count=S}=C) -> NS=S+1,
    raw_append(M,Feed),
    C#writer{cache=M,count=NS}.

feed_key(M,Feed) -> <<(list_to_binary(lists:concat(["/",kvs_rocks:format(Feed),"/"])))/binary,(term_to_binary(id(M)))/binary>>.
raw_append(M,Feed) -> rocksdb:put(ref(), feed_key(M,Feed), term_to_binary(M), [{sync,true}]).

remove(Rec,Feed) ->
   kvs:ensure(#writer{id=Feed}),
   W = #writer{count=C} = kvs:writer(Feed),
   {ok,I} = rocksdb:iterator(ref(), []),
   case kvs:delete(Feed,id(Rec)) of
        ok -> Count = C - 1,
              kvs:save(W#writer{count = Count, cache = I}),
              Count;
         _ -> C end.

append(Rec,Feed) ->
   kvs:ensure(#writer{id=Feed}),
   Id = element(2,Rec),
   W = kvs:writer(Feed),
   case kvs:get(Feed,Id) of
        {ok,_}    -> raw_append(Rec,Feed), kvs:save(W#writer{cache=Rec,count=W#writer.count + 1}), Id;
        {error,_} -> kvs:save(kvs:add(W#writer{args=Rec,cache=Rec})), Id end.

prev(_,_,_,_,_,_,N,C) when C == N -> C;
prev(I,Key,S,{ok,A,X},_,T,N,C) -> prev(I,Key,S,A,X,T,N,C);
prev(_,___,_,{error,_},_,_,_,C) -> C;
prev(I,Key,S,A,_,_,N,C) when size(A) > S ->
     case binary:part(A,0,S) of Key ->
          rocksdb:delete(ref(), A, []),
          Next = rocksdb:iterator_move(I, prev),
          prev(I,Key, S, Next, [], A, N, C + 1);
                                  _ -> C end;
prev(_,_,_,_,_,_,_,C) -> C.

cut(Feed,Id) ->
    Key    = list_to_binary(lists:concat(["/",kvs_rocks:format(Feed),"/"])),
    A      = <<Key/binary,(term_to_binary(Id))/binary>>,
    {ok,I} = rocksdb:iterator(ref(), []),
    case rocksdb:iterator_move(I, {seek,A}) of
         {ok,A,X} -> {ok,prev(I,Key,size(Key),A,X,[],-1,0)};
                _ -> {error,not_found} end.
