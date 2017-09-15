-module(kvs_stream).
-include("kvs.hrl").
-include("user.hrl").
-compile(export_all).

new(Tab) -> #cursor{feed=kvs:next_id(cursor,1),tab=Tab}.

take(top,Number,Cursor)   -> take(next,Number,top(Cursor),[]);
take(bot,Number,Cursor)   -> take(prev,Number,bot(Cursor),[]).
take(___,_,{error,_},Res) -> lists:flatten(Res);
take(___,0,Cursor,Res)    -> lists:flatten(Res);
take(Atom,Number,#cursor{val=Body}=Cursor,Res) ->
    take(Atom,Number-1,?MODULE:Atom(Cursor),[Body|Res]).

add(top,Message,#cursor{tab=Table,top=Top,val=[]}=Cursor) ->
    Id = element(2,Message),
    M1 = setelement(#iterator.next, Message, Top),
    M2 = setelement(#iterator.prev, M1,      []),
    kvs:put(M2),
    Cursor#cursor{val=M2,id=Id,bot=Id,top=Id};

add(bot,Message,#cursor{tab=Table,bot=Bot,val=[]}=Cursor) ->
    Id = element(2,Message),
    M1 = setelement(#iterator.prev, Message, Bot),
    M2 = setelement(#iterator.next, M1,      []),
    kvs:put(M2),
    Cursor#cursor{val=M2,id=Id,bot=Id,top=Id};

add(top,Message,#cursor{tab=Table,top=Top,val=Body}=Cursor) when element(2, Body) /= Top ->
    add(top,Message,top(Cursor));

add(bot,Message,#cursor{tab=Table,bot=Bot,val=Body}=Cursor) when element(2, Body) /= Bot ->
    add(bot,Message,bot(Cursor));

add(top,Message,#cursor{tab=Table,bot=[],top=Top,val=Body}=Cursor) ->
    Id = element(2,Message),
    M1 = setelement(#iterator.next, Message, Top),
    M2 = setelement(#iterator.prev, M1,      []),
    kvs:put(M2),
    Cursor#cursor{val=M2,id=Id,top=Id,bot=Id};

add(bot,Message,#cursor{tab=Table,bot=Bot,top=[],val=Body}=Cursor) ->
    Id = element(2,Message),
    M1 = setelement(#iterator.prev, Message, Bot),
    M2 = setelement(#iterator.next, M1,      []),
    kvs:put(M2),
    Cursor#cursor{val=M2,id=Id,bot=Id,top=Id};

add(top,Message,#cursor{tab=Table,top=Top,val=Body}=Cursor) ->
    Id = element(2,Message),
    M1 = setelement(#iterator.next, Message, Top),
    M2 = setelement(#iterator.prev, M1,      []),
    M3 = setelement(#iterator.prev, Body,    Id),
    kvs:put(M2), kvs:put(M3),
    Cursor#cursor{val=M2,id=Id,top=Id};

add(bot,Message,#cursor{tab=Table,bot=Bot,val=Body}=Cursor) ->
    Id = element(2,Message),
    M1 = setelement(#iterator.prev, Message, Bot),
    M2 = setelement(#iterator.next, M1,      []),
    M3 = setelement(#iterator.next, Body,    Id),
    kvs:put(M2), kvs:put(M3),
    Cursor#cursor{val=M2,id=Id,bot=Id}.

top(#cursor{top=Top}=Cursor) -> seek(Top,Cursor).
bot(#cursor{bot=Bot}=Cursor) -> seek(Bot,Cursor).

seek(Id, #cursor{tab=Tab}=Cursor) ->
    case kvs:get(Tab,Id) of
         {ok, Record} -> Tab = element(1,Record),
                         Id = element(2,Record),
                         Cursor#cursor{id=Id,val=Record} end.

next(#cursor{tab=Table,id=Id,val=Body}=Cursor) ->
    Next = element(#iterator.next,Body),
    case kvs:get(Table,Next) of
         {ok, Record} -> Cursor#cursor{id=element(2,Record),val=Record};
         {error, Err} -> {error,Err} end.

prev(#cursor{tab=Table,id=Id,val=Body}=Cursor) ->
    Next = element(#iterator.prev,Body),
    case kvs:get(Table,Next) of
         {ok, Record} -> Cursor#cursor{id=element(2,Record),val=Record};
         {error, Err} -> {error,Err} end.

save(Cursor) ->
    kvs:put(Cursor), Cursor.

load(#cursor{feed=Key}) ->
    kvs:get(cursor,Key).

test() ->
    A = save(
        add(top,#user{id=kvs:next_id(user,1)},
        add(bot,#user{id=kvs:next_id(user,1)},
        add(top,#user{id=kvs:next_id(user,1)},
        add(bot,#user{id=kvs:next_id(user,1)},
        new(user)))))),
    X = take(top,-1,A),
    Y = take(bot,-1,A),
    X = lists:reverse(Y),
    length(X).
