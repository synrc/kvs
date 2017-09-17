-module(kvs_stream).
-include("kvs.hrl").
-include("user.hrl").
-compile(export_all).
-export([ new/0, top/1, bot/1, take/2, load/1, save/1, seek/2, next/1, prev/1, add/2, remove/2, down/1, up/1 ]).

% PUBLIC

new()                 -> #cur{id=kvs:next_id(cur,1)}.
up(C)                 -> C#cur{dir=0}.
down(C)               -> C#cur{dir=1}.
top(#cur{top=[]}=C)   -> C#cur{val=[]};
top(#cur{top=T}=C)    -> seek(T,C).
bot(#cur{bot=[]}=C)   -> C#cur{val=[]};
bot(#cur{bot=B}=C)    -> seek(B,C).
add(M,#cur{dir=D}=C) when element(2,M) == [] -> add(dir(D),si(M,kvs:next_id(tab(M),1)),C);
add(M,#cur{dir=D}=C)  -> add(dir(D),M,C).
save(#cur{}=C)        -> kvs:put(C), C.
load(K)               -> case kvs:get(cur,K) of {ok,C} -> C; E -> E end.
next(#cur{pos=[]}=C)  -> {error,[]};
next(#cur{pos=B}=C)   -> {L,R} = right(C), lookup(kvs:get(tab(B),en(B)),C,{L,R}).
prev(#cur{pos=[]}=C)  -> {error,[]};
prev(#cur{pos=B}=C)   -> {L,R} = left(C), lookup(kvs:get(tab(B),ep(B)),C,{L,R}).
take(N,#cur{dir=D}=C)    -> take(acc(D),N,C,[]).
seek(I,  #cur{val=[]}=C) -> {error,[]};
seek(I,   #cur{val=B}=C) -> {ok,R}=kvs:get(tab(B),I), C#cur{pos=R}.
rewind(#cur{val=[]}=C)   -> {error,[]};
rewind(#cur{dir=D,top=T,bot=B,val=V}=C) -> {ok,R}=kvs:get(tab(V),select(D,T,B)), C#cur{val=R}.
remove(I,#cur{val=[]}=C) -> {error,val};
remove(I, #cur{val=B,pos=X}=C) ->
    {ok,R}=kvs:get(tab(B),I), kvs:delete(tab(B),I),
    join(id(R)/=id(X),[fix(tab(B),X)||X<-[ep(R),en(R)]],C).

% PRIVATE

add(bot,M,#cur{bot=T,val=[]}=C) ->
    Id=id(M), N=sn(sp(M,T),[]), kvs:put(N),
    C#cur{val=N,pos=N,bot=Id,top=Id};

add(top,M,#cur{top=B,val=[]}=C) ->
    Id=id(M), N=sp(sn(M,B),[]), kvs:put(N),
    C#cur{val=N,pos=N,top=Id,bot=Id};

add(top,M,#cur{top=T, val=V}=C) when element(2,V)/=T ->
    add(top, M, rewind(C));

add(bot,M,#cur{bot=B, val=V}=C) when element(2,V)/=B ->
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

cas(true,  L, R) -> L;
cas(false, L, R) -> R.

join(F,[[],[]],C) ->
    {X,Y} = dec(C),
    C#cur{top=[],bot=[],val=[],pos=[],left=X,right=Y};

join(F,[[], R],#cur{pos=P}=C) ->
    N=sp(R,[]), kvs:put(N), {X,Y} = dec(C),
    C#cur{top=id(N), val=N, pos=cas(F,P,N), left=X, right=Y};

join(F,[L, []],#cur{pos=P}=C) ->
    N=sn(L,[]), kvs:put(N), {X,Y} = dec(C),
    C#cur{bot=id(N), val=N, left=X, pos=cas(F,P,N), right=Y};

join(F,[L,  R],#cur{pos=P}=C) ->
    N=sp(R,id(L)), kvs:put([N,sn(L,id(R))]), {X,Y} = dec(C),
    C#cur{val=N, left=X, pos=cas(F,P,N), right=Y}.

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

lookup({ok,R},C,{X,Y})      -> C#cur{pos=R,left=X,right=Y};
lookup({error,X},C,_)       -> {error,X}.
take(_,_,{error,_},R)       -> lists:flatten(R);
take(_,0,_,R)               -> lists:flatten(R);
take(A,N,#cur{pos=B}=C,R)   -> take(A,N-1,?MODULE:A(C),[B|R]).
swap(1,{L,R})               -> {R,L};
swap(0,{L,R})               -> {L,R}.
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
    test1(),
    test2(),
    create_destroy(),
    next_prev_duality(),
    test_sides(),
    rewind(),
    ok.

rewind() ->
    C = #cur{top=T,bot=B,left=L,right=R,val=V,pos=P} =
    save(add(#person{},down(
    add(#person{},up(
    add(#person{},down(
    add(#person{},up(
    add(#person{},new())))))))))),
    PId = id(P),
    VId = id(V),
    B = VId,
    PId = B - 4,
    ok.

test_sides() ->
    #cur{top=T,bot=B,left=L,right=R,val=V,pos=P} =
    add(#person{},up(
    add(#person{},down(
    add(#person{},new()))))),
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
        add(#person{id=A},
        add(#person{id=B},
        add(#person{id=C},
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
         add(#person{id=A},
         add(#person{id=B},
         add(#person{id=C},
         add(#person{id=D},
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
         add(#person{id=D},
         add(#person{id=C},
         add(#person{id=B},
         add(#person{id=A},
         up(new())))))))))).

test1() ->
    Cur = new(),
    [A,B,C,D] = [ kvs:next_id(person,1) || _ <- lists:seq(1,4) ],
    R  = save(add(#person{id=D},
              add(#person{id=C},
              add(#person{id=B},
              add(#person{id=A}, Cur ))))),
    X  = take(-1,up(bot(R))),
    Y  = take(-1,down(top(R))),
    X  = lists:reverse(Y),
    L  = length(X).
