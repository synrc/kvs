-module(kvs_stream).
-include("kvs.hrl").
-include("user.hrl").
-compile(export_all).
-export([ new/0, top/1, bot/1, take/2, load/1, save/1, seek/2, next/1, prev/1, add/2, remove/2, down/1, up/1 ]).

% PUBLIC

new()                 -> #cur{id=kvs:next_id(cur,1)}.
down(C)               -> C#cur{dir=0}.
up(C)                 -> C#cur{dir=1}.
top(#cur{top=[]}=C)   -> C#cur{val=[]};
top(#cur{top=T}=C)    -> seek(T,C).
bot(#cur{bot=[]}=C)   -> C#cur{val=[]};
bot(#cur{bot=B}=C)    -> seek(B,C).
add(M,#cur{dir=D}=C) when element(2,M) == [] -> add(dir(D),si(M,kvs:next_id(tab(M),1)),C);
add(M,#cur{dir=D}=C)  -> add(dir(D),M,C).
save(#cur{}=C)        -> kvs:put(C), C.
load(K)               -> case kvs:get(cur,K) of {ok,C} -> C; E -> E end.
next(#cur{val=[]}=C)  -> {error,[]};
next(#cur{val=B}=C)   -> {L,R} = right(C), lookup(kvs:get(tab(B),en(B)),C,{L,R}).
prev(#cur{val=[]}=C)  -> {error,[]};
prev(#cur{val=B}=C)   -> {L,R} = left(C), lookup(kvs:get(tab(B),ep(B)),C,{L,R}).
take(N,#cur{dir=D}=C)    -> take(acc(D),N,?MODULE:(dir(D))(C),[]).
seek(I,  #cur{val=[]}=C) -> {error,[]};
seek(I,   #cur{val=B}=C) -> {ok,R}=kvs:get(tab(B),I), C#cur{val=R}.
remove(I,#cur{val=[]}=C) -> {error,val};
remove(I, #cur{val=B}=C) -> {ok,R}=kvs:get(tab(B),I), kvs:delete(tab(B),I),
                            join([fix(tab(B),X)||X<-[ep(R),en(R)]],C).

% PRIVATE

add(top,M,#cur{top=T,val=[]}=C) -> Id=id(M), N=sp(sn(M,T),[]), kvs:put(N),     C#cur{val=N,bot=Id,top=Id};
add(bot,M,#cur{bot=B,val=[]}=C) -> Id=id(M), N=sn(sp(M,B),[]), kvs:put(N),     C#cur{val=N,bot=Id,top=Id};
add(top,M,#cur{top=T, val=V}=C) when element(2,V)/=T -> n2o:warning(?MODULE,"cur:~p",[V]), add(top, M, top(C));
add(bot,M,#cur{bot=B, val=V}=C) when element(2,V)/=B -> n2o:warning(?MODULE,"cur:~p",[V]), add(bot, M, bot(C));
add(top,M,#cur{top=T, val=V}=C) -> Id=id(M), N=sp(sn(M,T),[]), kvs:put([N,sp(V,Id)]), {L,R} = inc(C), C#cur{val=N,top=Id,left=L,right=R};
add(bot,M,#cur{bot=B, val=V}=C) -> Id=id(M), N=sn(sp(M,B),[]), kvs:put([N,sn(V,Id)]), {L,R} = inc(C), C#cur{val=N,bot=Id,left=L,right=R}.

join([[],[]],C) ->                                          {X,Y} = dec(C), C#cur{top=[],bot=[],   val=[],left=X,right=Y};
join([[], R],C) -> N=sp(R,[]),    kvs:put(N),               {X,Y} = dec(C), C#cur{top=id(N),       val=N, left=X,right=Y};
join([L, []],C) -> N=sn(L,[]),    kvs:put(N),               {X,Y} = dec(C), C#cur{       bot=id(N),val=N, left=X,right=Y};
join([L,  R],C) -> N=sp(R,id(L)), kvs:put([N,sn(L,id(R))]), {X,Y} = dec(C), C#cur{                 val=N, left=X,right=Y}.

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
acc(0)  -> next;
acc(1)  -> prev.

fix(M,[])   -> [];
fix(M,X)    -> fix(kvs:get(M,X)).
fix({ok,O}) -> O;
fix(_)      -> [].

lookup({ok,R},C,{X,Y})      -> C#cur{val=R,left=X,right=Y};
lookup({error,X},C,_)       -> {error,X}.
take(_,_,{error,_},R)       -> lists:flatten(R);
take(_,0,_,R)               -> lists:flatten(R);
take(A,N,#cur{val=B}=C,R)   -> take(A,N-1,?MODULE:A(C),[B|R]).
inc(#cur{left=L,right=R})   -> {L,R+1}.
dec(#cur{left=0,right=0})   -> {0,0};
dec(#cur{left=L,right=0})   -> {L-1,0};
dec(#cur{left=0,right=R})   -> {0,R-1};
dec(#cur{left=L,right=R})   -> {L,R-1}.
right(#cur{left=0,right=0}) -> {0,0};
right(#cur{left=L,right=0}) -> {L,0};
right(#cur{left=L,right=R}) -> {L+1,R-1}.
left(#cur{left=0,right=0})  -> {0,0};
left(#cur{left=0,right=R})  -> {0,R};
left(#cur{left=L,right=R})  -> {L-1,R+1}.

% TESTS

check() ->
    test1(),
    test2(),
    create_destroy(),
    next_prev_duality(),
    ok.

next_prev_duality() ->
    Cur = new(),
    [A,B,C] = [ kvs:next_id(person,1) || _ <- lists:seq(1,3) ],
    R = kvs_stream:save(
        kvs_stream:add(#person{id=A},
        kvs_stream:add(#person{id=B},
        kvs_stream:add(#person{id=C},
        Cur)))),
    X = kvs_stream:load(id(Cur)),
    X = kvs_stream:prev(
        kvs_stream:prev(
        kvs_stream:next(
        kvs_stream:next(X)))).

test2() ->
    Cur = new(),
    [A,B,C,D] = [ kvs:next_id(person,1) || _ <- lists:seq(1,4) ],
    R =      add(#person{id=A},
        down(add(#person{id=B},
          up(add(#person{id=C},
        down(add(#person{id=D},
          up(Cur)))))))),
    X = remove(A,
        remove(B,
        remove(C,
        remove(D,R)))),
    [] = take(-1,up(bot(X))).

create_destroy() ->
    Cur = new(),
    [A,B,C,D] = [ kvs:next_id(person,1)
             || _ <- lists:seq(1,4) ],
    Y  = kvs_stream:remove(B,
         kvs_stream:remove(D,
         kvs_stream:remove(A,
         kvs_stream:remove(C,
         kvs_stream:add(#person{id=A},
         kvs_stream:down(
         kvs_stream:add(#person{id=B},
         kvs_stream:up(
         kvs_stream:add(#person{id=C},
         kvs_stream:down(
         kvs_stream:add(#person{id=D},
         kvs_stream:up(
         kvs_stream:new())))))))))))),
    [] = kvs_stream:take(-1,
         kvs_stream:down(
         kvs_stream:top(Y))).

test1() ->
    Cur = new(),
    take(-1,down(top(Cur))),
    [A,B,C,D] = [ kvs:next_id(person,1) || _ <- lists:seq(1,4) ],
    R = save(add(top,#person{id=A},
        add(bot,#person{id=B},
        add(top,#person{id=C},
        add(bot,#person{id=D}, Cur ))))),
    X = take(-1,down(top(R))),
    Y = take(-1,up(bot(R))),
    n2o:info(?MODULE,"X:~w",[{R,X}]),
    n2o:info(?MODULE,"Y:~w",[Y]),
    X = lists:reverse(Y),
    L = length(X).
