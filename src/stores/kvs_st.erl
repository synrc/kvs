-module(kvs_st).
-description('KVS STREAM NATIVE ROCKS').
-include("kvs.hrl").
-include("stream.hrl").
-include("metainfo.hrl").
-export(?STREAM).
-import(kvs_rocks, [key/2, key/1, bt/1, ref/0]).

% section: kvs_stream prelude

se(X,Y,Z) -> setelement(X,Y,Z).
e(X,Y) -> element(X,Y).
c4(R,V) -> se(#reader.args,  R, V).
si(M,T) -> se(#it.id, M, T).
id(T) -> e(#it.id, T).

% section: next, prev
feed(Feed) -> take((reader(Feed))#reader{args=-1}).

top(#reader{}=C) -> C#reader{dir=1}.
bot(#reader{}=C) -> C#reader{dir=0}.

% handle -> seek -> move
move_it(Key,Dir) -> 
  Seek = fun(F,{ok,H})            -> {F(H,{seek,Key}),H};
            (F,{{ok,_,_},H})      -> F(H,Dir);
            (F,{{ok,_}, H})       -> F(H,Dir);
            (_,{error,Error})     -> {error,Error};
            (_,{{error,Error},_}) -> {error,Error};
            (F,{R,O})             -> F(R,O) end,

  case lists:foldl(Seek, {ref(),[]},
    [fun rocksdb:iterator/2, fun rocksdb:iterator_move/2, fun rocksdb:iterator_move/2]) of
    {ok,_,Bin} -> {ok,bt(Bin)};
    {error, Error} -> {error,Error}
  end.

% iterator -> specific feed reader
read_it(C, Feed, Move) ->
  case Move of 
    {ok, Bin} when element(1,Bin) =:= Feed -> C#reader{cache=Bin};
    {ok,_} -> C;
    {error, Error} -> {error, Error}
  end.

next(#reader{cache=[]}) -> {error,empty};
next(#reader{feed=Feed,cache=I}=C) when is_tuple(I) -> read_it(C,Feed,move_it(key(Feed,I),next)).

prev(#reader{cache=[]}) -> {error,empty};
prev(#reader{cache=I,feed=Feed}=C) when is_tuple(I) -> read_it(C,Feed,move_it(key(Feed,I),prev)).

% section: take, drop

drop(#reader{args=N}) when N < 0 -> #reader{};
drop(#reader{args=N}=C) when N == 0 -> C;
drop(#reader{args=N,feed=Feed,cache=I}=C) when N > 0 ->
   Key = key(Feed),
   {ok, H} = rocksdb:iterator(ref(), []),
   First = rocksdb:iterator_move(H, {seek,Key}),

   Term  = lists:foldl(
    fun (_,{{ok,K,_},{_,X}}) when N > X -> {K,{<<131,106>>,N}};
        (_,{{ok,K,Bin},{A,X}}) when N =< X->
           case binary:part(K,0,size(Key)) of
                Key -> {rocksdb:iterator_move(H,next),{Bin,X+1}};
                  _ -> {{error,range},{A,X}} end;
        (_,{_,{_,_}}) -> {[],{<<131,106>>,N}}
     end,
           {First,{<<131,106>>,1}},
           lists:seq(0,N)),
   C#reader{cache=bt(element(1,element(2,Term)))}.

%  1. Курсор всегда выставлен на следущий невычитанный элемент
%  2. Если после вычитки курсор указывает на недавно вычитаный элемент -- это признак конца списка
%  3. Если результат вычитки меньше требуемого значения -- это признак конца списка
%  4. Если курсор установлен в конец списка и уже вернул его последний элемент
%     то результат вычитки будет равным пустому списку


take(#reader{pos='end',dir=0}=C) -> C#reader{args=[]}; % 4
take(#reader{args=N,feed=Feed,cache={T,O},dir=0}=C) -> % 1
   Key = key(Feed),
   {ok,I} = rocksdb:iterator(ref(), []),
   {ok,K,BERT} = rocksdb:iterator_move(I, {seek,key(Feed,{T,O})}),
   {KK,Res} = kvs_rocks:next2(I,Key,size(Key),K,BERT,[],case N of -1 -> -1; J -> J + 1 end,0),
   Last = last(KK,O,'end'),
   case {Res,length(Res)} of
        {[],_} -> C#reader{args=[],cache=[]};
        {[H],  _A} when element(2,KK) == O -> C#reader{args=Res,pos=Last,cache={e(1,H),e(2,H)}}; % 2
        {[H|_X],A} when A < N + 1 orelse N == -1 -> C#reader{args=Res,cache={e(1,H),e(2,H)},pos=Last};
        {[H| X],A} when A == N -> C#reader{args=[bt(BERT)|X],cache={e(1,H),e(2,H)},pos=Last};
        {[H|_X],A} when A =< N andalso Last == 'end'-> C#reader{args=Res,cache={e(1,H),e(2,H)},pos=Last};
        {[H| X],_} -> C#reader{args=X,cache={e(1,H),e(2,H)}} end;

take(#reader{pos=0,dir=0}=C)       -> C#reader{pos='begin',args=[]};
take(#reader{pos='begin',dir=1}=C) -> C#reader{args=[]}; % 4

% TODO: try to remove lists:reverse and abstract both branches
take(#reader{args=N,feed=Feed,cache={T,O},dir=1}=C) -> % 1
   Key = key(Feed),
   {ok,I} = rocksdb:iterator(ref(), []),
   {ok,K,BERT} = rocksdb:iterator_move(I, {seek,key(Feed,{T,O})}),
   {KK,Res} = kvs_rocks:prev2(I,Key,size(Key),K,BERT,[],case N of -1 -> -1; J -> J + 1 end,0),
   Last = last(KK,O,'begin'),
   case {lists:reverse(Res),length(Res)} of
        {[],_} -> C#reader{args=[],cache=[]};
        {[H],_} when element(2,KK) == O -> C#reader{args=Res,pos=Last,cache={e(1,H),e(2,H)}}; % 2
        {[_|_],A} when A < N - 1 orelse N == -1 -> [HX|_] = Res, C#reader{args=Res,cache={e(1,HX),e(2,HX)},pos=Last};
        {[_|X],A} when A == N -> [HX|_] = Res, C#reader{args=[bt(BERT)|X],cache={e(1,HX),e(2,HX)},pos=Last};
        {[_|_],A} when A =< N andalso Last == 'begin'-> [HX|_] = Res, C#reader{args=lists:reverse(Res),cache={e(1,HX),e(2,HX)},pos=Last};
        {[_|_],_} -> [HX|TL] = Res, C#reader{args=lists:reverse(TL),cache={e(1,HX),e(2,HX)}} end.

last(KK,O,Atom) ->
   Last = case KK of
      [] -> Atom;
      _ when element(2,KK) == O -> Atom;
      _ -> 0
   end,
   Last.

% new, save, load, up, down, top, bot

load_reader(Id) ->
    case kvs:get(reader,Id) of
         {ok,#reader{}=C} -> C;
              _ -> #reader{id=[]} end.

writer(Id) -> case kvs:get(writer,Id) of {ok,W} -> W; {error,_} -> #writer{id=Id} end.
reader(Id) ->
    case kvs:get(writer,Id) of
         {ok,#writer{id=Feed}} ->
             Key = key(Feed),
             {ok,I} = rocksdb:iterator(ref(), []),
             {ok,_,BERT} = rocksdb:iterator_move(I, {seek,Key}),
             F = bt(BERT),
             #reader{id=kvs:seq([],[]),feed=Id,cache={e(1,F),e(2,F)}};
         {error,_} -> #reader{} end.
save(C) -> NC = c4(C,[]), kvs:put(NC), NC.

% add

add(#writer{args=M}=C) when element(2,M) == [] -> add(si(M,kvs:seq([],[])),C);
add(#writer{args=M}=C) -> add(M,C).

add(M,#writer{id=Feed,count=S}=C) -> NS=S+1, raw_append(M,Feed), C#writer{cache=M,count=NS}.

remove(Rec,Feed) ->
   kvs:ensure(#writer{id=Feed}),
   W = #writer{count=C} = kvs:writer(Feed),
   {ok,I} = rocksdb:iterator(ref(), []),
   case kvs:delete(Feed,id(Rec)) of
        ok -> Count = C - 1,
              kvs:save(W#writer{count = Count, cache = I}),
              Count;
         _ -> C end.

raw_append(M,Feed) ->
   rocksdb:put(ref(), key(Feed,M), term_to_binary(M), [{sync,true}]).

append(Rec,Feed) ->
   kvs:ensure(#writer{id=Feed}),
   Id = e(2,Rec),
   W = kvs:writer(Feed),
   case kvs:get(Feed,Id) of
        {ok,_} -> raw_append(Rec,Feed), kvs:save(W#writer{cache=Rec,count=W#writer.count + 1}), Id;
        {error,_} -> kvs:save(kvs:add(W#writer{args=Rec,cache=Rec})), Id end.

cut(Feed,Id) ->
    Key    = key(Feed),
    A      = key(Feed,Id),
    {ok,I} = rocksdb:iterator(ref(), []),
    case rocksdb:iterator_move(I, {seek,A}) of
         {ok,A,X} -> {ok,kvs_rocks:cut(I,Key,size(Key),A,X,[],-1,0)};
                _ -> {error,not_found} end.
