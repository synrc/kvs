-module(kvs_stream).
-description('KVS STREAM').
-copyrihgt('Synrc Research Center').
-author('Maxim Sokhatsky').
-license('ISC').
-include_lib("kvs/include/kvs.hrl").
-compile(export_all).
%-include_lib("stdlib/include/assert.hrl").

% section: kvs_stream prelude

se(X,Y,Z)  -> setelement(X,Y,Z).
set(X,Y,Z) -> setelement(X,Z,Y).
e(X,Y)  -> element(X,Y).
c0(R,V) -> se(1, R, V).
c1(R,V) -> se(#reader.id,    R, V).
c2(R,V) -> se(#reader.pos,   R, V).
c3(R,V) -> se(#reader.cache, R, V).
c4(R,V) -> se(#reader.args,  R, V).
c5(R,V) -> se(#reader.feed,  R, V).
c6(R,V) -> se(#reader.dir,   R, V).
wf(R,V) -> se(#writer.first, R, V).
sn(M,T) -> se(#iter.next, M, T).
sp(M,T) -> se(#iter.prev, M, T).
si(M,T) -> se(#iter.id, M, T).
sf(M,T) -> se(#iter.feed, M, T).
el(X,T) -> e(X, T).
tab(T)  -> e(1, T).
id(T)   -> e(#iter.id, T).
en(T)   -> e(#iter.next, T).
ep(T)   -> e(#iter.prev, T).
pos(T)  -> e(#reader.pos, T).
args(T) -> e(#writer.args, T).
dir(0)  -> top;
dir(1)  -> bot.
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
w({ok,#writer{first=B}},bot,C)            -> C#reader{cache={tab(B),id(B)},pos=1};
w({ok,#writer{cache=B,count=Size}},top,C) -> C#reader{cache={tab(B),id(B)},pos=Size};
w({error,X},_,_)                          -> {error,X}.

% section: take, drop

drop(#reader{dir=D,cache=B,args=N,pos=P}=C) -> drop(acc(D),N,C,C,P,B).
take(#reader{dir=D,cache=B,args=N,pos=P}=C) -> take(acc(D),N,C,C,[],P,B).

take(_,_,{error,_},C2,R,P,B) -> C2#reader{args=lists:flatten(R),pos=P,cache=B};
take(_,0,_,C2,R,P,B)         -> C2#reader{args=lists:flatten(R),pos=P,cache=B};
take(A,N,#reader{cache={T,I},pos=P}=C,C2,R,_,_) ->
    take(A,N-1,?MODULE:A(C),C2,[element(2,kvs:get(T,I))|R],P,{T,I}).

drop(_,_,{error,_},C2,P,B)     -> C2#reader{pos=P,cache=B};
drop(_,0,_,C2,P,B)             -> C2#reader{pos=P,cache=B};
drop(A,N,#reader{cache=B,pos=P}=C,C2,_,_) ->
    drop(A,N-1,?MODULE:A(C),C2,P,B).

% new, save, load, up, down, top, bot

load_writer (Id) -> case kvs:get(writer,Id) of {ok,C} -> C; E -> E end.
load_reader (Id) -> case kvs:get(reader,Id) of {ok,C} -> C; E -> E end.
writer (Id) -> #writer{id=Id}.
reader (Id) ->
    case kvs:get(writer,Id) of
         {ok,#writer{first=[]}} -> #reader{id=kvs:next_id(reader,1),feed=Id,cache=[]};
         {ok,#writer{first=F}}  -> #reader{id=kvs:next_id(reader,1),feed=Id,cache={tab(F),id(F)}};
         {error,X} -> {error,X} end.
save (C) -> NC = c4(C,[]), kvs:put(NC), NC.
up   (C) -> C#reader{dir=0}.
down (C) -> C#reader{dir=1}.

% add

add(#writer{args=M}=C) when element(2,M) == [] -> add(si(M,kvs:next_id(tab(M),1)),C);
add(#writer{args=M}=C) -> add(M,C).

add(M,#writer{cache=[]}=C) ->
    _Id=id(M), N=sp(sn(M,[]),[]), kvs:put(N),
    C#writer{cache=N,count=1,first=N};

add(M,#writer{cache=V,count=S}=C) ->
    N=sp(sn(M,[]),id(V)), P=sn(V,id(M)), kvs:put([N,P]),
    C#writer{cache=N,count=S+1}.

% tests

check() -> test1().

test1() ->
    Id  = {p2p,1,2},
    X   = 5,
    _W   = kvs_stream:save(kvs_stream:writer(Id)),
    #reader{id=R1} = kvs_stream:save(kvs_stream:reader(Id)),
    #reader{id=R2} = kvs_stream:save(kvs_stream:reader(Id)),
    [ kvs_stream:save(
      kvs_stream:add((
      kvs_stream:load_writer(Id))
      #writer{args={user2,[],[],[],[],[],[],[],[],[]}})) || _ <- lists:seq(1,X) ],
    Bot = kvs_stream:bot(kvs_stream:load_reader(R1)),
    Top = kvs_stream:top(kvs_stream:load_reader(R2)),
    #reader{args=F} = kvs_stream:take(Bot#reader{args=20,dir=0}),
    #reader{args=B} = kvs_stream:take(Top#reader{args=20,dir=1}),
    true = (X == length(F)),
    true = (F == lists:reverse(B)).
