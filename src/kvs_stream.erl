-module(kvs_stream).
-description('KVS STREAM').
-copyrihgt('Synrc Research Center').
-author('Maxim Sokhatsky').
-license('ISC').
-include("kvs.hrl").
-export([ new/0, top/1, bot/1, take/2, load/1, save/1, down/1, up/1,
          check/0, seek/1, rewind/1, next/1, prev/1, add/2, remove/2 ]).

% section: kvs_stream prelude

se(X,Y,Z) -> setelement(X,Y,Z).
e(X,Y)  -> element(X,Y).
cv(R,V) -> se(#cur.val,   R, V).
cb(R,V) -> se(#cur.bot,   R, V).
ct(R,V) -> se(#cur.top,   R, V).
cl(R,V) -> se(#cur.left,  R, V).
cr(R,V) -> se(#cur.right, R, V).
cd(R,V) -> se(#cur.dir,   R, V).
sn(M,T) -> se(#iter.next, M, T).
sp(M,T) -> se(#iter.prev, M, T).
si(M,T) -> se(#iter.id, M, T).
el(X,T) -> e(X, T).
tab(T)  -> e(1, T).
et(T)   -> e(#cur.top, T).
eb(T)   -> e(#cur.bot, T).
id(T)   -> e(#iter.id, T).
en(T)   -> e(#iter.next, T).
ep(T)   -> e(#iter.prev, T).
dir(0)  -> top;
dir(1)  -> bot.
acc(0)  -> prev;
acc(1)  -> next.

% section: next, prev

next (#cur{pos=[]}=C) -> {error,[]};
next (#cur{pos=B} =C) -> pos(kvs:get(tab(B),en(B)),C,right(C)).
prev (#cur{pos=[]}=C) -> {error,[]};
prev (#cur{pos=B} =C) -> pos(kvs:get(tab(B),ep(B)),C,left(C)).

left (#cur{left=0,right=0,dir=D}) -> swap(D,{0,  0});
left (#cur{left=0,right=R,dir=D}) -> swap(D,{0,  R});
left (#cur{left=L,right=R,dir=D}) -> swap(D,{L-1,R+1}).
right(#cur{left=0,right=0,dir=D}) -> swap(D,{0,  0});
right(#cur{left=L,right=0,dir=D}) -> swap(D,{L,  0});
right(#cur{left=L,right=R,dir=D}) -> swap(D,{L+1,R-1}).

swap(1,{L,R}) -> {R,L};
swap(0,{L,R}) -> {L,R}.

pos({ok,R},C,{X,Y}) -> C#cur{pos=R,left=X,right=Y};
pos({error,X},C,_)  -> {error,X}.

% section: take

take(N,#cur{dir=D}=C)  -> take(acc(D),N,C,[]).

take(_,_,{error,_},R)     -> lists:flatten(R);
take(_,0,_,R)             -> lists:flatten(R);
take(A,N,#cur{pos=B}=C,R) -> take(A,N-1,?MODULE:A(C),[B|R]).

% rewind

rewind(#cur{val=[]}=C) -> {error,[]};
rewind(#cur{dir=D,top=T,bot=B,val=V}=C) ->
    C#cur{val=id(kvs:get(tab(V),select(D,T,B)))}.

select(0,T,B) -> T;
select(1,T,B) -> B;
select(P,P,X) -> X;
select(P,N,X) -> N.

% seek

seek(#cur{val=[]}=C) -> C;
seek(#cur{bot=X,pos=P,dir=0}=C) when element(2,P) == X -> C;
seek(#cur{top=X,pos=P,dir=1}=C) when element(2,P) == X -> C;
seek(#cur{top=T,bot=B,left=L,right=R,dir=0,pos=P}=C) ->
     C#cur{pos=id(kvs:get(tab(P),B)),left=0,right=L+R};
seek(#cur{top=T,bot=B,left=L,right=R,dir=1,pos=P}=C) ->
     C#cur{pos=id(kvs:get(tab(P),T)),left=L+R,right=0}.

% new, save, load, up, down, top, bot

new   () -> #cur{id=kvs:next_id(cur,1)}.
save (C) -> kvs:put(C), C.
load (K) -> case kvs:get(cur,K) of {ok,C} -> C; E -> E end.
up   (C) -> C#cur{dir=0}.
down (C) -> C#cur{dir=1}.
top  (C) -> seek(up(C)).
bot  (C) -> seek(down(C)).

% add

add(M,#cur{dir=D}=C) when element(2,M) == [] ->
    add(dir(D),si(M,kvs:next_id(tab(M),1)),C);
add(M,#cur{dir=D}=C) ->
    add(dir(D),M,C).

inc(#cur{left=L,right=R,dir=D}) -> swap(D,{L+1,R}).

add(bot,M,#cur{bot=T,val=[]}=C) ->
    Id=id(M), N=sn(sp(M,T),[]), kvs:put(N),
    C#cur{val=N,pos=N,bot=Id,top=Id};

add(top,M,#cur{top=B,val=[]}=C) ->
    Id=id(M), N=sp(sn(M,B),[]), kvs:put(N),
    C#cur{val=N,pos=N,top=Id,bot=Id};

add(top,M,#cur{top=T, val=V}=C) when element(2,V) /= T ->
    add(top, M, rewind(C));

add(bot,M,#cur{bot=B, val=V}=C) when element(2,V) /= B ->
    add(bot, M, rewind(C));

add(bot,M,#cur{bot=T,val=V,pos=P}=C) ->
    Id=id(M), H=sn(sp(M,T),[]), N=sn(V,Id), kvs:put([H,N]),
    {L,R} = inc(C), C#cur{pos=select(V,P,N),val=H,bot=Id,left=L,right=R};

add(top,M,#cur{top=B,val=V,pos=P}=C) ->
    Id=id(M), H=sp(sn(M,B),[]), N=sp(V,Id), kvs:put([H,N]),
    {L,R} = inc(C), C#cur{pos=select(V,P,N),val=H,top=Id,left=L,right=R}.

% remove

remove(I,#cur{val=[]}=C)      -> {error,val};
remove(I,#cur{val=B,pos=X}=C) -> {ok,R}=kvs:get(tab(B),I), kvs:delete(tab(B),I),
                                 join(I,[fix(tab(B),X)||X<-[ep(R),en(R)]],C).

fix(M,[])     -> [];
fix(M,X)      -> fix(kvs:get(M,X)).
fix({ok,O})   -> O;
fix(_)        -> [].

dec(#cur{left=0,right=0,dir=D}) -> swap(D,{0,  0});
dec(#cur{left=L,right=0,dir=D}) -> swap(D,{L-1,0});
dec(#cur{left=0,right=R,dir=D}) -> swap(D,{0,R-1});
dec(#cur{left=L,right=R,dir=D}) -> swap(D,{L-1,R}).

m(I,_,I,_,I,L,R,P,V) -> {R,R};
m(_,I,I,_,I,L,R,P,V) -> {R,L};
m(I,_,_,I,I,L,R,P,V) -> {L,R};
m(_,I,_,I,I,L,R,P,V) -> {L,L};
m(I,_,_,_,I,L,R,P,V) -> {sn(V,id(R)),P};
m(_,I,_,_,I,L,R,P,V) -> {sp(V,id(R)),P};
m(_,_,I,_,I,L,R,P,V) -> {V,sn(P,id(L))};
m(_,_,_,I,I,L,R,P,V) -> {V,sp(P,id(L))};
m(_,_,_,_,I,L,R,P,V) -> {V,P}.

join(I,[[],[]],C) ->
    {X,Y} = dec(C),
    C#cur{top=[],bot=[],val=[],pos=[],left=X,right=Y};

join(I,[[], R],#cur{pos=P,val=V}=Cur) ->
    N=sp(R,[]), kvs:put(N), {X,Y} = dec(Cur),
    {NV,NP} = m(en(V),ep(V),en(P),ep(P),I,[],N,P,V),
    Cur#cur{top=id(N), val=NV, pos=NP, left=X, right=Y};

join(I,[L, []],#cur{pos=P,val=V}=Cur) ->
    N=sn(L,[]), kvs:put(N), {X,Y} = dec(Cur),
    {NV,NP} = m(en(V),ep(V),en(P),ep(P),I,N,[],P,V),
    Cur#cur{bot=id(N), val=NV, left=X, pos=NP, right=Y};

join(I,[L,  R],#cur{pos=P,val=V}=Cur) ->
    N=sp(R,id(L)), M=sn(L,id(R)), kvs:put([N,M]), {X,Y} = dec(Cur),
    {NV,NP} = m(en(V),ep(V),en(P),ep(P),I,N,M,P,V),
    Cur#cur{left=X, pos=NP, val=NV, right=Y}.

% TESTS

check() ->
    te_remove(),
    test1(),
    test2(),
    create_destroy(),
    next_prev_duality(),
    test_sides(),
    rewind(),
    ok.

rewind() ->
    Empty = {'user2',[],[],[],[],[],[],[],[]},
    C = #cur{top=T,bot=B,left=L,right=R,val=V,pos=P} =
    save(
    add(Empty,down(
    add(Empty,up(
    add(Empty,down(
    add(Empty,up(
    add(Empty,new())))))))))),
    PId = id(P),
    VId = id(V),
    B = VId,
    PId = B - 4,
    ok.

test_sides() ->
    Empty = {'user2',[],[],[],[],[],[],[],[]},
    #cur{top=T,bot=B,left=L,right=R,val=V,pos=P} =
    save(
    add(Empty,up(
    add(Empty,down(
    add(Empty,new())))))),
    PId = id(P),
    VId = id(V),
    VId = T,
    PId = B - 1,
    1 = T - B,
    L = R = 1.

next_prev_duality() ->
    Cur = new(),
    [A,B,C] = [ kvs:next_id('user2',1) || _ <- lists:seq(1,3) ],
    R = save(
        add({'user2',A,[],[],[],[],[],[],[]},
        add({'user2',B,[],[],[],[],[],[],[]},
        add({'user2',C,[],[],[],[],[],[],[]},
        Cur)))),
    X = load(id(Cur)),
    X = next(
        next(
        prev(
        prev(X)))).

test2() ->
    Cur = new(),
    [A,B,C,D] = [ kvs:next_id('user2',1) || _ <- lists:seq(1,4) ],
    [] = take(-1,
         up(
         bot(
         remove(A,
         remove(B,
         remove(C,
         remove(D,
         add({'user2',A,[],[],[],[],[],[],[]},
         add({'user2',B,[],[],[],[],[],[],[]},
         add({'user2',C,[],[],[],[],[],[],[]},
         add({'user2',D,[],[],[],[],[],[],[]},
         up(Cur)))))))))))).

create_destroy() ->
    Cur = new(),
    [A,B,C,D] = [ kvs:next_id('user2',1)
             || _ <- lists:seq(1,4) ],
    [] = take(-1,
         remove(B,
         remove(D,
         remove(A,
         remove(C,
         add({'user2',D,[],[],[],[],[],[],[]},
         add({'user2',C,[],[],[],[],[],[],[]},
         add({'user2',B,[],[],[],[],[],[],[]},
         add({'user2',A,[],[],[],[],[],[],[]},
         up(new())))))))))).

test1() ->
    [A,B,C,D] = [ kvs:next_id('user2',1) || _ <- lists:seq(1,4) ],
    R  = save(
         add({'user2',D,[],[],[],[],[],[],[]},
         add({'user2',C,[],[],[],[],[],[],[]},
         add({'user2',B,[],[],[],[],[],[],[]},
         add({'user2',A,[],[],[],[],[],[],[]},
         new() ))))),
    X  = take(-1,up(top(R))),
    Y  = take(-1,down(bot(R))),
    X  = lists:reverse(Y),
    L  = length(X).

te_remove() ->
    #cur{id=S}=save(new()),
    P = {'user2',[],[],[],[],[],[],[],[]},
    S1 = save(
         add(P,
         add(P,
         add(P,
         add(P,
         load(S)))))),
    4 = length(take(-1,S1)),
    S2 = save(top(S1)),
    S3 = save(remove(S2#cur.top-1,S2)),
    List = take(-1,top(S3)),
    Rev  = take(-1,bot(S3)),
    List = lists:reverse(Rev),
    3 = length(List),
    {S3,List}.
