-module(kvs_stream).
-description('KVS STREAM LAYER').
-include("kvs.hrl").
-include("stream.hrl").
-include("metainfo.hrl").
-export(?STREAM).
-export([metainfo/0]).

db() -> kvs:db().

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

n({ok,R},C,P)    -> r(kvs:get(tab(R),en(R)),C,P);
n({error,X},_,_) -> {error,X}.

p({ok,R},C,P)    -> r(kvs:get(tab(R),ep(R)),C,P);
p({error,X},_,_) -> {error,X}.

r({ok,R},C,P)    -> C#reader{cache={tab(R),id(R)},pos=P};
r({error,X},_,_) -> {error,X}.

w({ok,#writer{first=[]}},bot,C)           -> C#reader{cache=[],pos=1,dir=0};
w({ok,#writer{first=B}},bot,C)            -> C#reader{cache={tab(B),id(B)},pos=1,dir=0};
w({ok,#writer{cache=B,count=Size}},top,C) -> C#reader{cache={tab(B),id(B)},pos=Size,dir=1};
w({error,X},_,_)                          -> {error,X}.

% next, prev, top, bot
top(#reader{}=X)          -> top(X,db()).
top(#reader{feed=F}=C,Db) -> w(kvs:get(writer,F,Db),top,C).
bot(#reader{}=X)          -> bot(X,db()).
bot(#reader{feed=F}=C,Db) -> w(kvs:get(writer,F,Db),bot,C).

next(#reader{}=X)         -> next(X,db()).
next(#reader{cache=[]},_) -> {error,empty};
next(#reader{cache={T,R},pos=P}=C,Db) -> n(kvs:get(T,R,Db),C,P+1).

prev(#reader{}=X)       -> prev(X,db()).
prev(#reader{cache=[]},_) -> {error,empty};
prev(#reader{cache={T,R},pos=P}=C,Db) -> p(kvs:get(T,R,Db),C,P-1).

% take, drop, feed

drop(#reader{}=X)           -> drop(X,db()).
drop(#reader{cache=[]}=C,_) -> C#reader{args=[]};
drop(#reader{dir=D,cache=B,args=N,pos=P}=C,Db)  -> drop(acc(D),N,C,C,P,B,Db).
take(#reader{}=X)           -> take(X,db()).
take(#reader{cache=[]}=C,_) -> C#reader{args=[]};
take(#reader{dir=D,cache=_B,args=N,pos=P}=C,Db)  -> take(acc(D),N,C,C,[],P,Db).

take(_,_,{error,_},C2,R,P,_) -> C2#reader{args=lists:flatten(R),pos=P,cache={tab(hd(R)),en(hd(R))}};
take(_,0,_,C2,R,P,_)         -> C2#reader{args=lists:flatten(R),pos=P,cache={tab(hd(R)),en(hd(R))}};
take(A,N,#reader{cache={T,I},pos=P}=C,C2,R,_,Db) -> take(A,N-1,?MODULE:A(C,Db),C2,[element(2,kvs:get(T,I,Db))|R],P,Db).

drop(_,_,{error,_},C2,P,B,_)     -> C2#reader{pos=P,cache=B};
drop(_,0,_,C2,P,B,_)             -> C2#reader{pos=P,cache=B};
drop(A,N,#reader{cache=B,pos=P}=C,C2,_,_,Db) -> drop(A,N-1,?MODULE:A(C,Db),C2,P,B,Db).

feed(Feed)    -> feed(Feed,db()).
feed(Feed,Db) -> #reader{args=Args} = take((reader(Feed,Db))#reader{args=-1},Db), Args.

% new, save, load, writer, reader

load_reader (Id) -> load_reader(Id,db()).
load_reader (Id,Db) -> case kvs:get(reader,Id,Db) of {ok,C} -> C; _ -> #reader{id=[]} end.

save (C) -> save(C,db()).
save (C,Db) -> NC = c4(C,[]), kvs:put(NC,Db), NC.
writer (Id) -> writer(Id,db()).
writer (Id,Db) -> case kvs:get(writer,Id,Db) of {ok,W} -> W; {error,_} -> #writer{id=Id} end.
reader (Id) -> reader(Id,db()).
reader (Id,Db) -> case kvs:get(writer,Id,Db) of
         {ok,#writer{first=[]}} -> #reader{id=kvs:seq(reader,1),feed=Id,cache=[]};
         {ok,#writer{first=F}}  -> #reader{id=kvs:seq(reader,1),feed=Id,cache={tab(F),id(F)}};
         {error,_} -> save(#writer{id=Id},Db), reader(Id,Db) end.

% add, remove, append

add(#writer{}=X) -> add(X,db()).
add(#writer{args=M}=C,Db) when element(2,M) == [] -> add(si(M,kvs:seq(tab(M),1)),C,Db);
add(#writer{args=M}=C,Db) -> add(M,C,Db).

add(M,#writer{cache=[]}=C,Db) ->
    _Id=id(M), N=sp(sn(M,[]),[]), kvs:put(N,Db),
    C#writer{cache=N,count=1,first=N};

add(M,#writer{cache=V1,count=S}=C,Db) ->
    {ok,V} = kvs:get(tab(V1),id(V1),Db),
    N=sp(sn(M,[]),id(V)), P=sn(V,id(M)), kvs:put([N,P],Db),
    C#writer{cache=N,count=S+1}.

remove(Rec,Feed)  -> remove(Rec,Feed,db()).
remove(_Rec,Feed,Db) ->
   {ok,W=#writer{count=Count}} = kvs:get(writer,Feed,Db),
   NC = Count-1,
   kvs:save(W#writer{count=NC},Db),
   NC.

append(Rec,Feed)    -> append(Rec,Feed,db()).
append(Rec,Feed,Db) ->
   kvs:ensure(#writer{id=Feed},Db),
   Name = element(1,Rec),
   Id = element(2,Rec),
   case kvs:get(Name,Id,Db) of
        {ok,_}    -> Id;
        {error,_} -> kvs:save(kvs:add((kvs:writer(Feed,Db))#writer{args=Rec},Db),Db), Id end.
