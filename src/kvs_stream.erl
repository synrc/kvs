-module(kvs_stream).
-include("kvs.hrl").
-export([
    new/0, top/1, bot/1, take/2, load/1, save/1, down/1, up/1,
    check/0, seek/1, rewind/1, next/1, prev/1, add/2, remove/2 ]).

% PUBLIC

new()   -> #cur{id=kvs:next_id(cur,1)}.
up(C)   -> C#cur{dir=0}.
down(C) -> C#cur{dir=1}.
top(C)  -> seek(up(C)).
bot(C)  -> seek(down(C)).

seek(#cur{bot=[],dir=0}=C) -> C#cur{val=[]};
seek(#cur{top=[],dir=1}=C) -> C#cur{val=[]};
seek(#cur{bot=[],dir=0}=C) -> {error,[]};
seek(#cur{top=[],dir=1}=C) -> {error,[]};
seek(#cur{bot=X,pos=P,dir=0}=C) when element(2,P) == X -> C;
seek(#cur{top=X,pos=P,dir=1}=C) when element(2,P) == X -> C;
seek(#cur{top=T,bot=B,left=L,right=R,dir=0,pos=P}=C) ->
    C#cur{pos=id(kvs:get(tab(P),B)),left=0,right=L+R};
seek(#cur{top=T,bot=B,left=L,right=R,dir=1,pos=P}=C) ->
    C#cur{pos=id(kvs:get(tab(P),T)),left=L+R,right=0}.

rewind(#cur{val=[]}=C) -> {error,[]};
rewind(#cur{dir=D,top=T,bot=B,val=V}=C) ->
    C#cur{val=id(kvs:get(tab(V),select(D,T,B)))}.

add(M,#cur{dir=D}=C) when element(2,M) == [] ->
    add(dir(D),si(M,kvs:next_id(tab(M),1)),C);
add(M,#cur{dir=D}=C) -> add(dir(D),M,C).

save(C) -> kvs:put(C), C.
load(K) -> case kvs:get(cur,K) of {ok,C} -> C; E -> E end.

next(#cur{pos=[]}=C) -> {error,[]};
next(#cur{pos=B} =C) -> pos(kvs:get(tab(B),en(B)),C,right(C)).
prev(#cur{pos=[]}=C) -> {error,[]};
prev(#cur{pos=B} =C) -> pos(kvs:get(tab(B),ep(B)),C,left(C)).

take(N,#cur{dir=D}=C) -> take(acc(D),N,C,[]).

remove(I,#cur{val=[]}=C) -> {error,val};
remove(I, #cur{val=B,pos=X}=C) ->
    {ok,R}=kvs:get(tab(B),I), kvs:delete(tab(B),I),
    join(I,[fix(tab(B),X)||X<-[ep(R),en(R)]],C).

% PRIVATE

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
    Id=id(M), H=sn(sp(M,T),[]), N=sn(V,Id),
    kvs:put([H,N]), {L,R} = inc(C),
    C#cur{pos=select(V,P,N),val=H,bot=Id,left=L,right=R};

add(top,M,#cur{top=B,val=V,pos=P}=C) ->
    Id=id(M), H=sp(sn(M,B),[]), N=sp(V,Id),
    kvs:put([H,N]), {L,R} = inc(C),
    C#cur{pos=select(V,P,N),val=H,top=Id,left=L,right=R}.

select(0,T,B) -> T;
select(1,T,B) -> B;
select(P,P,X) -> X;
select(P,N,X) -> N.

join(I,[[],[]],C) ->
    {X,Y} = dec(C),
    C#cur{top=[],bot=[],val=[],pos=[],left=X,right=Y};

join(I,[[], R],#cur{pos=P,val=V}=Cur) ->
    N=sp(R,[]), kvs:put(N), {X,Y} = dec(Cur),
    [A,B,C,D] = [en(V),ep(V),en(P),ep(P)],
    {NV,NP} = m(A,B,C,D,I,[],N,P,V),
    Cur#cur{top=id(N), val=NV, pos=NP, left=X, right=Y};

join(I,[L, []],#cur{pos=P,val=V}=Cur) ->
    N=sn(L,[]), kvs:put(N), {X,Y} = dec(Cur),
    [A,B,C,D] = [en(V),ep(V),en(P),ep(P)],
    {NV,NP} = m(A,B,C,D,I,N,[],P,V),
    Cur#cur{bot=id(N), val=NV, left=X, pos=NP, right=Y};

join(I,[L,  R],#cur{pos=P,val=V}=Cur) ->
    N=sp(R,id(L)), M=sn(L,id(R)), kvs:put([N,M]), {X,Y} = dec(Cur),
    [A,B,C,D] = [en(V),ep(V),en(P),ep(P)],
    {NV,NP} = m(A,B,C,D,I,N,M,P,V),
    Cur#cur{left=X, pos=NP, val=NV, right=Y}.

m(I,_,I,_,I,L,R,P,V) -> {R,R};
m(_,I,I,_,I,L,R,P,V) -> {R,L};
m(I,_,_,I,I,L,R,P,V) -> {L,R};
m(_,I,_,I,I,L,R,P,V) -> {L,L};
m(I,_,_,_,I,L,R,P,V) -> {sn(V,id(R)),P};
m(_,I,_,_,I,L,R,P,V) -> {sp(V,id(R)),P};
m(_,_,I,_,I,L,R,P,V) -> {V,sn(P,id(L))};
m(_,_,_,I,I,L,R,P,V) -> {V,sp(P,id(L))};
m(_,_,_,_,I,L,R,P,V) -> {V,P}.

cv(R,V) -> setelement(#cur.val,   R, V).
cb(R,V) -> setelement(#cur.bot,   R, V).
ct(R,V) -> setelement(#cur.top,   R, V).
cl(R,V) -> setelement(#cur.left,  R, V).
cr(R,V) -> setelement(#cur.right, R, V).
cd(R,V) -> setelement(#cur.dir,   R, V).
sn(M,T) -> setelement(#iter.next, M, T).
sp(M,T) -> setelement(#iter.prev, M, T).
si(M,T) -> setelement(#iter.id, M, T).

el(X,T) -> element(X, T).
tab(T)  -> element(1, T).
et(T)   -> element(#cur.top, T).
eb(T)   -> element(#cur.bot, T).
id(T)   -> element(#iter.id, T).
en(T)   -> element(#iter.next, T).
ep(T)   -> element(#iter.prev, T).
dir(0)  -> top;
dir(1)  -> bot.
acc(0)  -> prev;
acc(1)  -> next.

fix(M,[])     -> [];
fix(M,X)      -> fix(kvs:get(M,X)).
fix({ok,O})   -> O;
fix(_)        -> [].

pos({ok,R},C,{X,Y})       -> C#cur{pos=R,left=X,right=Y};
pos({error,X},C,_)        -> {error,X}.
take(_,_,{error,_},R)     -> lists:flatten(R);
take(_,0,_,R)             -> lists:flatten(R);
take(A,N,#cur{pos=B}=C,R) -> take(A,N-1,?MODULE:A(C),[B|R]).
swap(1,{L,R})             -> {R,L};
swap(0,{L,R})             -> {L,R}.

inc(#cur{left=L,right=R,dir=D})   -> swap(D,{L+1,R}).
dec(#cur{left=0,right=0,dir=D})   -> swap(D,{0,0});
dec(#cur{left=L,right=0,dir=D})   -> swap(D,{L-1,0});
dec(#cur{left=0,right=R,dir=D})   -> swap(D,{0,R-1});
dec(#cur{left=L,right=R,dir=D})   -> swap(D,{L-1,R}).
right(#cur{left=0,right=0,dir=D}) -> swap(D,{0,0});
right(#cur{left=L,right=0,dir=D}) -> swap(D,{L,0});
right(#cur{left=L,right=R,dir=D}) -> swap(D,{L+1,R-1}).
left(#cur{left=0,right=0,dir=D})  -> swap(D,{0,0});
left(#cur{left=0,right=R,dir=D})  -> swap(D,{0,R});
left(#cur{left=L,right=R,dir=D})  -> swap(D,{L-1,R+1}).

% TESTS

check() ->
    te(),
    test1(),
    test2(),
    create_destroy(),
    next_prev_duality(),
    test_sides(),
    rewind(),
    ok.

rewind() ->
    Empty = {person,[],[],[],[],[],[],[],[]},
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
    Empty = {person,[],[],[],[],[],[],[],[]},
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
    [A,B,C] = [ kvs:next_id(person,1) || _ <- lists:seq(1,3) ],
    R = save(
        add({person,A,[],[],[],[],[],[],[]},
        add({person,B,[],[],[],[],[],[],[]},
        add({person,C,[],[],[],[],[],[],[]},
        Cur)))),
    X = load(id(Cur)),
    X = next(
        next(
        prev(
        prev(X)))).

test2() ->
    Cur = new(),
    [A,B,C,D] = [ kvs:next_id(person,1) || _ <- lists:seq(1,4) ],
    [] = take(-1,
         up(
         bot(
         remove(A,
         remove(B,
         remove(C,
         remove(D,
         add({person,A,[],[],[],[],[],[],[]},
         add({person,B,[],[],[],[],[],[],[]},
         add({person,C,[],[],[],[],[],[],[]},
         add({person,D,[],[],[],[],[],[],[]},
         up(Cur)))))))))))).

create_destroy() ->
    Cur = new(),
    [A,B,C,D] = [ kvs:next_id(person,1)
             || _ <- lists:seq(1,4) ],
    [] = take(-1,
         remove(B,
         remove(D,
         remove(A,
         remove(C,
         add({person,D,[],[],[],[],[],[],[]},
         add({person,C,[],[],[],[],[],[],[]},
         add({person,B,[],[],[],[],[],[],[]},
         add({person,A,[],[],[],[],[],[],[]},
         up(new())))))))))).

test1() ->
    [A,B,C,D] = [ kvs:next_id(person,1) || _ <- lists:seq(1,4) ],
    R  = save(
         add({person,D,[],[],[],[],[],[],[]},
         add({person,C,[],[],[],[],[],[],[]},
         add({person,B,[],[],[],[],[],[],[]},
         add({person,A,[],[],[],[],[],[],[]},
         new() ))))),
    X  = take(-1,up(top(R))),
    Y  = take(-1,down(bot(R))),
    X  = lists:reverse(Y),
    L  = length(X).

te() ->
    #cur{id=S}=kvs_stream:save(kvs_stream:new()),
    P = {person,[],[],[],[],[],[],[],[]},
    S1 = kvs_stream:save(
    kvs_stream:add(P,
    kvs_stream:add(P,
    kvs_stream:add(P,
    kvs_stream:add(P,
    kvs_stream:load(S)))))),
    4 = length(kvs_stream:take(-1,S1)),
    S2 = kvs_stream:save(kvs_stream:top(S1)),
    S3 = kvs_stream:save(kvs_stream:remove(S2#cur.top-1,S2)),
    List = kvs_stream:take(-1,kvs_stream:seek(kvs_stream:up(S3))),
    Rev  = kvs_stream:take(-1,kvs_stream:seek(kvs_stream:down(S3))),
    List = lists:reverse(Rev),
    3 = length(List),
    {S3,List}.
