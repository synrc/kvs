-module(kvs_stream).
-include("kvs.hrl").
-include("user.hrl").
-compile(export_all).
-export([ new/1, top/1, bot/1, take/2, load/1, save/1, seek/2, next/1, prev/1, add/3 ]).

% PUBLIC

new(T)                -> #cur{feed=kvs:next_id(cur,1),tab=T}.
top(#cur{top=T}=C)    -> seek(T,C).
bot(#cur{bot=B}=C)    -> seek(B,C).
take(N,#cur{dir=D}=C) -> take(D,N,C,[]).
save(C)               -> kvs:put(C), C.
load(#cur{feed=K})    -> kvs:get(cur,K).

seek(Id, #cur{tab=T}=C)         -> {ok,R}=kvs:get(T,Id), C#cur{id=el(2,R),val=R}.
next(#cur{tab=T,id=Id,val=B}=C) -> lookup(kvs:get(T,en(B)),C).
prev(#cur{tab=T,id=Id,val=B}=C) -> lookup(kvs:get(T,ep(B)),C).

add(top,M,#cur{top=T,val=[]}=C) -> Id=el(2,M), M2=sp(sn(M,T),[]), kvs:put(M2), C#cur{val=M2,id=Id,bot=Id,top=Id};
add(bot,M,#cur{bot=B,val=[]}=C) -> Id=el(2,M), M2=sn(sp(M,B),[]), kvs:put(M2), C#cur{val=M2,id=Id,bot=Id,top=Id};
add(top,M,#cur{top=T, val=D}=C) when element(2,D) /=T -> add(top, M, top(C));
add(bot,M,#cur{bot=B, val=D}=C) when element(2,D) /=B -> add(bot, M, bot(C));
add(top,M,#cur{top=T, val=D}=C) -> Id=el(2,M), M2=sp(sn(M,T),[]), kvs:put([M2,sp(D,Id)]), C#cur{val=M2,id=Id,top=Id};
add(bot,M,#cur{bot=B, val=D}=C) -> Id=el(2,M), M2=sn(sp(M,B),[]), kvs:put([M2,sn(D,Id)]), C#cur{val=M2,id=Id,bot=Id}.

sn(M,T)   -> setelement(#iterator.next, M, T).
sp(M,T)   -> setelement(#iterator.prev, M, T).
el(X,T)   -> element(X,T).
tn(T)     -> element(1,T).
id(T)     -> element(2,T).
en(T)     -> element(#iterator.next, T).
ep(T)     -> element(#iterator.prev, T).
dir(next) -> top;
dir(prev) -> bot.
down(C)   -> C#cur{dir=next}.
up(C)     -> C#cur{dir=prev}.

lookup({ok,R},C) -> C#cur{id=el(2,R),val=R};
lookup(X,C)      -> X.

take(_,_,{error,_},R)     -> lists:flatten(R);
take(_,0,_,R)             -> lists:flatten(R);
take(A,N,#cur{val=B}=C,R) -> take(A,N-1,?MODULE:A(C),[B|R]).

test() ->
    #cur{feed = K} = C = new(user),
    Feed = lists:concat(["cur",K]),
    A = save(add(top,#user{id=kvs:next_id(Feed,1)},
             add(bot,#user{id=kvs:next_id(Feed,1)},
             add(top,#user{id=kvs:next_id(Feed,1)},
             add(bot,#user{id=kvs:next_id(Feed,1)}, C ))))),
    X = take(-1,down(top(A))),
    Y = take(-1,up(bot(A))),
    X = lists:reverse(Y),
    L = length(X),
    {ok,{id_seq,Feed,L}} = kvs:get(id_seq,Feed),
    ok.
