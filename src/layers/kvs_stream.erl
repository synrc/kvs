-module(kvs_stream).
-description('KVS STREAM LAYER').
-include("kvs.hrl").
-include("stream.hrl").
-include("metainfo.hrl").
-export(?STREAM).
-export([metainfo/0]).

% boot for sample

metainfo() -> #schema { name = kvs,    tables = tables() }.
tables() -> [ #table  { name = writer, fields = record_info(fields, writer) },
              #table  { name = reader, fields = record_info(fields, reader) } ].

% section: kvs_stream prelude

se(X,Y,Z)  -> setelement(X,Y,Z).
e(X,Y)  -> element(X,Y).
c4(R,V) -> se(#reader.args,  R, V).
sn(M,T) -> se(#iter.next, M, T).
sp(M,T) -> se(#iter.prev, M, T).
si(M,T) -> se(#iter.id, M, T).
tab(T)  -> e(1, T).
id(T)   -> e(#iter.id, T).
en(T)   -> e(#iter.next, T).
ep(T)   -> e(#iter.prev, T).
acc(0)  -> next;
acc(1)  -> prev.

% section: next, prev

top(#reader{feed=F}=C) -> w(kvs:get(writer,F),top,C).
bot(#reader{feed=F}=C) -> w(kvs:get(writer,F),bot,C).

next(#reader{cache=[]}) -> {error,empty};
next(#reader{cache={T,R},pos=P}=C) -> n(kvs:get(T,R),C,P+1).

prev(#reader{cache=[]}) -> {error,empty};
prev(#reader{cache={T,R},pos=P}=C) -> p(kvs:get(T,R),C,P-1).

n({ok,R},C,P)    -> r(kvs:get(tab(R),en(R)),C,P);
n({error,X},_,_) -> {error,X}.
p({ok,R},C,P)    -> r(kvs:get(tab(R),ep(R)),C,P);
p({error,X},_,_) -> {error,X}.
r({ok,R},C,P)    -> C#reader{cache={tab(R),id(R)},pos=P};
r({error,X},_,_) -> {error,X}.
w({ok,#writer{first=[]}},bot,C)           -> C#reader{cache=[],pos=1};
w({ok,#writer{first=B}},bot,C)            -> C#reader{cache={tab(B),id(B)},pos=1};
w({ok,#writer{cache=B,count=Size}},top,C) -> C#reader{cache={tab(B),id(B)},pos=Size};
w({error,X},_,_)                          -> {error,X}.

% section: take, drop

drop(#reader{cache=[]}=C) -> C#reader{args=[]};
drop(#reader{dir=D,cache=B,args=N,pos=P}=C)  -> drop(acc(D),N,C,C,P,B).
take(#reader{cache=[]}=C) -> C#reader{args=[]};
take(#reader{dir=D,cache=B,args=N,pos=P}=C)  -> take(acc(D),N,C,C,[],P).

take(_,_,{error,_},C2,R,P) -> C2#reader{args=lists:flatten(R),pos=P,cache={tab(hd(R)),en(hd(R))}};
take(_,0,_,C2,R,P)         -> C2#reader{args=lists:flatten(R),pos=P,cache={tab(hd(R)),en(hd(R))}};
take(A,N,#reader{cache={T,I},pos=P}=C,C2,R,_) -> take(A,N-1,?MODULE:A(C),C2,[element(2,kvs:get(T,I))|R],P).

drop(_,_,{error,_},C2,P,B)     -> C2#reader{pos=P,cache=B};
drop(_,0,_,C2,P,B)             -> C2#reader{pos=P,cache=B};
drop(A,N,#reader{cache=B,pos=P}=C,C2,_,_) -> drop(A,N-1,?MODULE:A(C),C2,P,B).

% new, save, load, up, down, top, bot

load_reader (Id) -> case kvs:get(reader,Id) of {ok,C} -> C; _ -> #reader{id=[]} end.

writer (Id) -> case kvs:get(writer,Id) of {ok,W} -> W; {error,_} -> #writer{id=Id} end.
reader (Id) -> case kvs:get(writer,Id) of
         {ok,#writer{first=[]}} -> #reader{id=kvs:seq(reader,1),feed=Id,cache=[]};
         {ok,#writer{first=F}}  -> #reader{id=kvs:seq(reader,1),feed=Id,cache={tab(F),id(F)}};
         {error,_} -> save(#writer{id=Id}), reader(Id) end.
save (C) -> NC = c4(C,[]), kvs:put(NC), NC.

% add

add(#writer{args=M}=C) when element(2,M) == [] -> add(si(M,kvs:seq(tab(M),1)),C);
add(#writer{args=M}=C) -> add(M,C).

add(M,#writer{cache=[]}=C) ->
    _Id=id(M), N=sp(sn(M,[]),[]), kvs:put(N),
    C#writer{cache=N,count=1,first=N};

%add(M,#writer{cache=V,count=S}=C) ->
%    N=sp(sn(M,[]),id(V)), P=sn(V,id(M)), kvs:put([N,P]),
%    C#writer{cache=N,count=S+1}.

add(M,#writer{cache=V1,count=S}=C) ->
    {ok,V} = kvs:get(tab(V1),id(V1)),
    N=sp(sn(M,[]),id(V)), P=sn(V,id(M)), kvs:put([N,P]),
    C#writer{cache=N,count=S+1}.

remove(Rec,Feed) ->
   {ok,W=#writer{count=Count}} = kvs:get(writer,Feed),
   NC = Count-1,
   kvs:save(W#writer{count=NC}),
   NC.

append(Rec,Feed) ->
   kvs:ensure(#writer{id=Feed}),
   Name = element(1,Rec),
   Id = element(2,Rec),
   case kvs:get(Name,Id) of
        {ok,_}    -> Id;
        {error,_} -> kvs:save(kvs:add((kvs:writer(Feed))#writer{args=Rec})), Id end.

cut(_Feed,Id) ->
   case kvs:get(writer,Id) of
        {ok,#writer{count=N}} -> {ok,N};
        {error,_} -> {error,not_found} end.
