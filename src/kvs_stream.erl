-module(kvs_stream).
-include("kvs.hrl").
-include("user.hrl").
-compile(export_all).
-export([ new/0, top/1, bot/1, take/2, load/1, save/1, seek/2, next/1, prev/1, add/2, remove/2 ]).

% PUBLIC

new() -> #cur{id=kvs:next_id(cur,1)}.
top({ok,#cur{}=C})    -> top(C);
top({error,X})        -> {error,X};
top(#cur{top=[]}=C)   -> C#cur{val=[]};
top(#cur{top=T}=C)    -> seek(T,C).
bot({ok,#cur{}=C})    -> bot(C);
bot({error,X})        -> {error,X};
bot(#cur{bot=[]}=C)   -> C#cur{val=[]};
bot(#cur{bot=B}=C)    -> seek(B,C).
add(M,#cur{dir=D}=C) when element(2,M) == [] -> add(dir(D),si(M,kvs:next_id(tab(M),1)),C);
add(M,#cur{dir=D}=C)  -> add(dir(D),M,C).
save(#cur{}=C)        -> kvs:put(C), C.
load(K)               -> case kvs:get(cur,K) of {ok,C} -> C; E -> E end.
next(#cur{val=[]}=C)  -> {error,[]};
next(#cur{val=B}=C)   -> lookup(kvs:get(tab(B),en(B)),C).
prev(#cur{val=[]}=C)  -> {error,[]};
prev(#cur{val=B}=C)   -> lookup(kvs:get(tab(B),ep(B)),C).
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
add(top,M,#cur{top=T, val=V}=C) -> Id=id(M), N=sp(sn(M,T),[]), kvs:put([N,sp(V,Id)]), C#cur{val=N,top=Id};
add(bot,M,#cur{bot=B, val=V}=C) -> Id=id(M), N=sn(sp(M,B),[]), kvs:put([N,sn(V,Id)]), C#cur{val=N,bot=Id}.

join([[],[]],C) ->                                          C#cur{top=[],bot=[],   val=[]};
join([[], R],C) -> N=sp(R,[]),    kvs:put(N),               C#cur{top=id(N),       val=N};
join([L, []],C) -> N=sn(L,[]),    kvs:put(N),               C#cur{       bot=id(N),val=N};
join([L,  R],C) -> N=sp(R,id(L)), kvs:put([N,sn(L,id(R))]), C#cur{                 val=N}.

sn(M,T) -> setelement(#iter.next, M, T).
sp(M,T) -> setelement(#iter.prev, M, T).
el(X,T) -> element(X, T).
tab(T)  -> element(1, T).
id(T)   -> element(#iter.id, T).
si(M,T) -> setelement(#iter.id, M, T).
en(T)   -> element(#iter.next, T).
ep(T)   -> element(#iter.prev, T).
dir(0)  -> top;
dir(1)  -> bot.
acc(0)  -> next;
acc(1)  -> prev.
down(C) -> C#cur{dir=0}.
up(C)   -> C#cur{dir=1}.

fix(M,[])   -> [];
fix(M,X)    -> fix(kvs:get(M,X)).
fix({ok,O}) -> O;
fix(_)      -> [].

lookup({ok,R},C)          -> C#cur{val=R};
lookup({error,X},C)       -> {error,C}.
take(_,_,{error,_},R)     -> lists:flatten(R);
take(_,0,_,R)             -> lists:flatten(R);
take(A,N,#cur{val=B}=C,R) -> take(A,N-1,?MODULE:A(C),[B|R]).

% TESTS

check() -> test1(), test2(), create_destroy(), ok.

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
    [] = take(-1,down(top(X))).

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
    X = lists:reverse(Y),
    L = length(X).
