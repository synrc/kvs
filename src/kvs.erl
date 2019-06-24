-module(kvs).
-copyright('Synrc Research Center s.r.o.').
-compile(export_all).
%-include_lib("stdlib/include/qlc.hrl").
-include("config.hrl").
-include("metainfo.hrl").
-include("kvs.hrl").
-include("user.hrl").
-include("api.hrl").

% Public Main Backend is given in sys.config and
% could be obtained with application:get_env(kvs,dba,store_mnesia).

delete(Table,Key)  -> delete  (Table, Key, #kvs{mod=?DBA}).
remove(Table,Key)  -> remove  (Table, Key, #kvs{mod=?DBA}).
get(Table,Key)     -> get     (Table, Key, #kvs{mod=?DBA}).
index(Table,K,V)   -> index   (Table, K,V, #kvs{mod=?DBA}).
change_storage(Table,Type) -> change_storage(Table,Type, #kvs{mod=?DBA}).
entries(A,B,C)     -> entries (A,B,C, #kvs{mod=?DBA}).
join()             -> join    ([],    #kvs{mod=?DBA}).
join(Node)         -> join    (Node,  #kvs{mod=?DBA}).
count(Table)       -> count   (Table, #kvs{mod=?DBA}).
add(Record)        -> add     (Record, #kvs{mod=?DBA}).
all(Table)         -> all     (Table, #kvs{mod=?DBA}).
put(Record)        -> put     (Record, #kvs{mod=?DBA}).
link(Record)       -> link    (Record, #kvs{mod=?DBA}).
unlink(Record)     -> unlink  (Record, #kvs{mod=?DBA}).
fold(Fun,Acc,T,S,C,D) -> fold (Fun,Acc,T,S,C,D, #kvs{mod=?DBA}).
info(T)            -> info    (T, #kvs{mod=?DBA}).
start()            -> start   (#kvs{mod=?DBA}).
stop()             -> stop    (#kvs{mod=?DBA}).
destroy()          -> destroy (#kvs{mod=?DBA}).
version()          -> version (#kvs{mod=?DBA}).
dir()              -> dir     (#kvs{mod=?DBA}).
seq(Table,DX) -> next_id(Table,DX).
next_id(Table,DX)  -> next_id(Table, DX, #kvs{mod=?DBA}).

generation(Table,Key) ->
    case Key - topleft(Table,Key) < norm(application:get_env(kvs,generation,{?MODULE,limit}),Table,Key) of
         true -> skip;
         false -> kvs:rotate(Table) end.

norm({A,B},Table,Key) -> A:B(Table,Key);
norm(_,Table,Key)     -> limit(Table,Key).

limit(user,_Key)       -> 25000;
limit(comment,_Key)    -> 25000;
limit(_Table,_Key)     -> 25000.

forbid(user)           -> 5;
forbid(comment)        -> 5;
forbid(____)           -> 5.

% Implementation

init(Backend, Module) ->
    [ begin
        Backend:create_table(T#table.name, [{attributes,T#table.fields},{T#table.copy_type, [node()]},{type,T#table.type}]),
        [ Backend:add_table_index(T#table.name, Key) || Key <- T#table.keys ],
        T
    end || T <- (Module:metainfo())#schema.tables ].

start(#kvs{mod=DBA}) -> DBA:start().
stop(#kvs{mod=DBA}) -> DBA:stop().
change_storage(Type) -> [ change_storage(Name,Type) || #table{name=Name} <- kvs:tables() ].
change_storage(Table,Type,#kvs{mod=DBA}) -> DBA:change_storage(Table,Type).
destroy(#kvs{mod=DBA}) -> DBA:destroy().
join(Node,#kvs{mod=DBA}) -> R = DBA:join(Node), rotate_new(), load_partitions(), {R,load_config()}.
version(#kvs{mod=DBA}) -> DBA:version().
tables() -> lists:flatten([ (M:metainfo())#schema.tables || M <- modules() ]).
table(Name) when is_atom(Name) -> lists:keyfind(rname(Name),#table.name,tables());
table(_) -> false.
dir(#kvs{mod=DBA}) -> DBA:dir().
info(T,#kvs{mod=DBA}) -> DBA:info(T).
modules() -> application:get_env(kvs,schema,[kvs_user, kvs_acl, kvs_feed, kvs_subscription ]).
containers() ->
    lists:flatten([ [ {T#table.name,T#table.fields}
        || T=#table{container=true} <- (M:metainfo())#schema.tables ]
    || M <- modules() ]).

create(ContainerName) -> create(ContainerName, kvs:next_id(atom_to_list(ContainerName), 1), #kvs{mod=?DBA}).

create(ContainerName, Id, Driver) ->
    kvs:info(?MODULE,"Create: ~p",[ContainerName]),
    Fields = proplists:get_value(ContainerName, kvs:containers()),
    Instance = list_to_tuple([ContainerName | lists:map(fun(_) -> [] end,Fields)]),
    Top  = setelement(#container.id,Instance,Id),
    Top2 = setelement(#container.top,Top,[]),
    Top3 = setelement(#container.rear,Top2,[]),
    Top4 = setelement(#container.count,Top3,0),
    ok = kvs:put(Top4, Driver),
    Id.

ensure_link(Record, #kvs{mod=_Store}=Driver) ->

    Id    = element(2,Record),
    Type  = rname(element(1,Record)),
    CName = element(#iterator.container,Record),
    Cid   = case element(#iterator.feed_id,Record) of
                      [] -> rname(element(1,Record));
               undefined -> rname(element(1,Record));
                     Fid -> Fid end,

    Container = case kvs:get(CName, Cid, Driver) of
        {ok,Res} -> Res;
        {error, _} when Cid /= undefined andalso Cid /= [] ->
            Fields = proplists:get_value(CName, kvs:containers()),
            NC1 = list_to_tuple([CName | lists:map(fun(_) -> [] end,Fields)]),
            NC2 = setelement(#container.id, NC1, Cid),
            NC3 = setelement(#container.rear, NC2, Id),
            setelement(#container.count, NC3, 0);
        _Error -> error end,

    case Container of
        error -> {error,no_container};
        _ when element(#container.top, Container) == Id -> {error,just_added};
            _ ->
                Top = case element(#container.top, Container) of
                    undefined -> [];
                           [] -> [];
                          Tid -> case kvs:get(Type, Tid, Driver) of
                                     {error, _} -> [];
                                     {ok,    T} -> setelement(#iterator.next, T, Id) end end,

                Prev = case Top of
                    undefined -> [];
                           [] -> [];
                            E -> element(#iterator.id, E) end,

                Next = [],

                C2 = setelement(#container.top, Container, Id),
                C3 = setelement(#container.count, C2, element(#container.count, Container)+1),

                R  = Record,

                R1 = setelement(#iterator.next,    R,  Next),
                R2 = setelement(#iterator.prev,    R1, Prev),
                R3 = setelement(#iterator.feed_id, R2, element(#container.id, Container)),

                case {kvs:put(R3, Driver),Top} of      % Iterator
                    {ok,[]}   -> kvs:put(C3, Driver);  % Container
                    {ok,Top}  -> kvs:put(C3, Driver),
                                 kvs:put(Top, Driver);
                           __ -> kvs:error(?MODULE,"Error Updating Iterator: ~p~n",
                                             [element(#container.id,R3)]) end,

                kvs:info(?MODULE,"Put: ~p~n", [element(#container.id,R3)]),

                {ok, R3} end.

link(Record,#kvs{mod=_Store}=Driver) ->
    Id = element(#iterator.id, Record),
    case kvs:get(rname(element(1,Record)), Id, Driver) of
              {ok, Exists} -> ensure_link(Exists, Driver);
        {error, not_found} -> {error, not_found} end.

%add(Record, #kvs{mod=store_mnesia}=Driver) when is_tuple(Record) -> store_mnesia:add(Record);
add(Record, #kvs{}=Driver) when is_tuple(Record) -> append(Record,Driver).

append(Record, #kvs{mod=_Store}=Driver) when is_tuple(Record) ->
    Id = element(#iterator.id, Record),
    Name = rname(element(1,Record)),
    generation(Name, Id),
    case kvs:get(Name, Id, Driver) of
                {error, _} -> ensure_link(Record, Driver);
         {aborted, Reason} -> {aborted, Reason};
                   {ok, _} -> {error, exist} end.

reverse(#iterator.prev) -> #iterator.next;
reverse(#iterator.next) -> #iterator.prev.

relink(C, E, D) ->
    kvs:warning(?MODULE,"Deprecated! Will be removed in the future, use unlink/3 instead",[]),
    unlink(C, E, D).
unlink(E,Driver) ->
    case kvs:get(element(#iterator.container,E),element(#iterator.feed_id,E)) of
        {ok,C} ->
            unlink(C, E, Driver),
            E2=setelement(#iterator.next, E, []),
            E3=setelement(#iterator.prev, E2, []),
            kvs:put(E3,Driver),
            {ok,E};
        _ -> {error,no_container} end.
unlink(Container, E, Driver) ->
    Id   = element(#iterator.id, E),
    Next = element(#iterator.next, E),
    Prev = element(#iterator.prev, E),
    case kvs:get(element(1,E), Prev, Driver) of
        {ok, PE} -> kvs:put(setelement(#iterator.next, PE, Next), Driver);
               _ -> ok end,
    case kvs:get(element(1,E), Next, Driver) of
        {ok, NE} -> kvs:put(setelement(#iterator.prev, NE, Prev), Driver);
               _ -> ok end,
    C2 = case element(#container.top, Container) of
        Id -> setelement(#container.top, Container, Prev);
         _ -> Container end,
    C3 = case element(#container.rear, C2) of
        Id -> setelement(#container.rear, C2, Next);
         _ -> C2 end,
    case element(#container.top,C3) of
        undefined -> kvs:delete(element(1,C3),element(#container.id,C3));
               [] -> kvs:delete(element(1,C3),element(#container.id,C3));
                _ -> kvs:put(setelement(#container.count,C3,element(#container.count,C3)-1), Driver) end.

delete(Tab, Key, #kvs{mod=Mod}) ->
    case range(Tab,Key) of
         [] -> Mod:delete(Tab, Key);
          T -> Mod:delete(T, Key) end.

remove(Record, Id,#kvs{mod=store_mnesia}) -> store_mnesia:remove(Record,Id);
remove(Record, Id,#kvs{}=Driver) -> takeoff(Record,Id,Driver).

takeoff(Record,Id,#kvs{}=Driver) ->
    case get(Record,Id) of
         {error, not_found} -> kvs:error(?MODULE,"Can't remove ~p~n",[{Record,Id}]);
                     {ok,R} -> do_remove(R,Driver) end.

do_remove(E,#kvs{}=Driver) ->
    case get(element(#iterator.container,E),element(#iterator.feed_id,E)) of
         {ok, C} -> unlink(C,E,Driver);
               _ -> skip end,
    kvs:info(?MODULE,"Delete: ~p", [E]),
    kvs:delete(element(1,E),element(2,E), Driver).

fold(___,Acc,_,[],_,_,_) -> Acc;
fold(___,Acc,_,undefined,_,_,_) -> Acc;
fold(___,Acc,_,_,0,_,_) -> Acc;
fold(Fun,Acc,Table,Start,Count,Direction,Driver) ->
    %io:format("fold: ~p~n",[{Table, Start, Driver}]),
    try
    case kvs:get(rname(Table), Start, Driver) of
         {ok, R} -> Prev = element(Direction, R),
                    Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
                    fold(Fun, Fun(R,Acc), Table, Prev, Count1, Direction, Driver);
           _Error -> %kvs:error(?MODULE,"Error: ~p~n",[Error]),
                    Acc end catch _:_ -> Acc end.

entries({error,_},_,_,_)      -> [];
entries({ok,T},N,C,Driver) ->
    lists:reverse(fold(fun(A,Acc) -> [A|Acc] end,[],N,element(#container.top,T),C,#iterator.prev,Driver)).
    

add_seq_ids() ->
    Init = fun(Key) ->
           case kvs:get(id_seq, Key) of
                {error, _} -> {Key,kvs:put(#id_seq{thing = Key, id = 0})};
                {ok, _} -> {Key,skip} end end,
    [ Init(atom_to_list(Name))  || {Name,_Fields} <- containers() ].


put(Records,#kvs{mod=Mod}) when is_list(Records) -> Mod:put(Records);

put(Record,#kvs{mod=Mod}) ->
    case range(element(1,Record),element(2,Record)) of
         [] -> Mod:put(Record);
         Name ->  Mod:put(setelement(1,Record,Name)) end.


get(RecordName, Key, #kvs{mod=Mod}) ->
    case range(RecordName,Key) of
         []   -> Mod:get(RecordName, Key);
         Name -> case Mod:get(Name, Key) of
                      {ok,Record} -> {ok,setelement(1,Record,kvs:last(RecordName,Key))};
                      Else -> Else end end.

count(Tab,#kvs{mod=DBA}) -> lists:foldl(fun(T,A) -> DBA:count(T) + A end, 0, rlist(Tab)).
all(Tab,#kvs{mod=DBA}) ->
    lists:flatten([ rnorm(rname(Tab),DBA:all(T)) || T <- rlist(Tab) ]).
index(Tab, Key, Value,#kvs{mod=DBA}) ->
    lists:flatten([ rnorm(rname(Tab),DBA:index(T, Key, Value)) || T <- rlist(Tab) ]).
next_id(Tab, Incr, KVX) when is_list(Tab) -> next_id(list_to_atom(Tab), Incr, KVX);
next_id(Tab, Incr,#kvs{mod=DBA}) ->
    DBA:next_id(case table(Tab) of #table{} -> atom_to_list(Tab); _ -> Tab end, Incr).

save_db(Path) ->
    Data = lists:append([all(B) || B <- [list_to_atom(Name) || {table,Name} <- kvs:dir()] ]),
    kvs:save(Path, Data).

load_db(Path) ->
    add_seq_ids(),
    AllEntries = kvs:load(Path),
    [kvs:put(E) || E <- lists:filter(fun(E) -> is_tuple(E) end ,AllEntries)].

save(Dir, Value) ->
    filelib:ensure_dir(Dir),
    file:write_file(Dir, term_to_binary(Value)).

load(Key) ->
    {ok, Bin} = file:read_file(Key),
    binary_to_term(Bin).

notify(_EventPath, _Data) -> skip.

config(Key)     -> config(kvs, Key, "").
config(App,Key) -> config(App,Key, "").
config(App, Key, Default) -> case application:get_env(App,Key) of
                                  undefined -> Default;
                                     {ok,V} -> V end.

log_modules() -> [].
-define(ALLOWED, (config(kvs,log_modules,kvs))).

log(Module, String, Args, Fun) ->
    case lists:member(Module,?ALLOWED:log_modules()) of
         true -> error_logger:Fun("~p:"++String, [Module|Args]);
         false -> skip end.

info(Module, String,   Args) -> log(Module, String, Args, info_msg).
warning(Module,String, Args) -> log(Module, String, Args, warning_msg).
error(Module, String,  Args) -> log(Module, String, Args, error_msg).


dump() -> dump([ rlist(N) || #table{name=N} <- kvs:tables() ]).
dump(short) ->
    Gen = fun(T) ->
        {S,M,C}=lists:unzip3([ dump_info(R) || R <- rlist(T) ]),
        {lists:usort(S),lists:sum(M),lists:sum(C)}
    end,
    dump_format([ {T,Gen(T)} || T <- [ N || #table{name=N} <- kvs:tables() ] ]);
dump(Table) when is_atom(Table) -> dump(rlist(Table));
dump(Tables) ->
    dump_format([{case nname(T) of 1 -> rname(T); _ -> T end,dump_info(T)} || T <- lists:flatten(Tables) ]).
dump_info(T) ->
    {mnesia:table_info(T,storage_type),
    mnesia:table_info(T,memory) * erlang:system_info(wordsize) / 1024 / 1024,
    mnesia:table_info(T,size)}.
dump_format(List) ->
    io:format("~20s ~32s ~14s ~10s~n~n",["NAME","STORAGE TYPE","MEMORY (MB)","ELEMENTS"]),
    [ io:format("~20s ~32w ~14.2f ~10b~n",[T,S,M,C]) || {T,{S,M,C}} <- List ],
    io:format("~nSnapshot taken: ~p~n",[calendar:now_to_datetime(os:timestamp())]).

% Table Partitions

range(RecordName,Id)   -> (find(kvs:config(kvs:rname(RecordName)),RecordName,Id))#block.name.
topleft(RecordName,Id) -> (find(kvs:config(kvs:rname(RecordName)),RecordName,Id))#block.left.
last(RecordName,Id)    -> (find(kvs:config(kvs:rname(RecordName)),RecordName,Id))#block.last.

find([],_,_Id) -> #block{left=1,right=infinity,name=[],last=[]};
find([Range|T],RecordName,Id) ->
     case lookup(Range,Id) of
          [] -> find(T,RecordName,Id);
          Range -> Range end.

lookup(#block{left=Left,right=Right}=I,Id) when Id =< Right, Id >= Left -> I;
lookup(#block{},_) -> [].

rotate_new() ->
    N = [ kvs:rotate(kvs:table(T)) || {T,_} <- fold_tables(),
        length(proplists:get_value(attributes,info(last_disc(T)),[])) /= length((table(rname(T)))#table.fields) ],
    %io:format("Nonexistent: ~p~n",[N]),
    N.
rotate(#table{name=N}) ->
    R = name(rname(N)),
    init(setelement(#table.name,kvs:table(kvs:last_table(N)),R)),
    update_config(rname(N),R);
rotate(Table) ->
    Ranges = kvs:config(Table),
    {M,F} = application:get_env(kvs,forbidding,{?MODULE,forbid}),
    New = lists:sublist(Ranges,M:F(Table)),
    
    Delete = Ranges -- New,
    io:format("Delete: ~p~n",[Delete]),
%    [ mnesia:change_table_copy_type(Name, node(), disc_only_copies) || #block{name=Name} <- shd(Delete) ],
    [ mnesia:delete_table(Name) || #block{name=Name} <- Delete ],
    rotate(kvs:table(Table)), ok.
load_partitions()  ->
    [ case kvs:get(config,Table) of
        {ok,{config,_,List}} -> application:set_env(kvs,Table,List);
        _Else -> ok end || {table,Table} <- kvs:dir() ].

rnorm(Tag,List) -> [ setelement(1,R,Tag) || R <- List ].
rlist(Table)   -> [ N || #block{name=N} <- kvs:config(Table) ]++[Table].
shd([])        -> [];
shd(X)         -> [hd(X)].
wait()         -> timer:tc(fun() -> mnesia:wait_for_tables([ T#table.name || T <- kvs:tables()],infinity) end).
stl([])        -> [];
stl(S)         -> tl(S).
dat(T)         -> [ mnesia:change_table_copy_type(Name, node(), disc_only_copies) || #block{name=Name} <- stl((element(2,kvs:get(config,T)))#config.value) ].
omitone(1)     -> [];
omitone(X)     -> X.
limit()        -> infinity.
load_config()  -> [ application:set_env(kvs,Key,Value) || #config{key=Key,value=Value}<- kvs:all(config) ].
store(Table,X) -> application:set_env(kvs,Table,X), X.
rname(Table)   -> list_to_atom(lists:filter(fun(X) -> not lists:member(X,"1234567890") end, atom_to_list(Table))).
nname(Table)   -> list_to_integer(case lists:filter(fun(X) -> lists:member(X,"1234567890") end, atom_to_list(Table)) of [] -> "1"; E -> E end).
fold(N)        -> kvs:fold(fun(X,A)->[X|A]end,[],process,N,-1,#iterator.next,#kvs{mod=store_mnesia}).
top(acl)       -> id_seq(conv);
top(i)         -> id_seq(conv);
top(Table)     -> id_seq(Table).
name(T)        -> list_to_atom(lists:concat([T,omitone(kvs:next_id(lists:concat([T,".tables"]),1))])).
init(T)        -> store_mnesia:create_table(T#table.name, [{attributes,T#table.fields},{T#table.copy_type, [node()]}]),
                [ store_mnesia:add_table_index(T#table.name, Key) || Key <- T#table.keys ].
id_seq(Tab)    -> T = atom_to_list(Tab), case kvs:get(id_seq,T) of {ok,#id_seq{id=Id}} -> Id; _ -> kvs:next_id(T,1) end.
last_disc(T)   -> list_to_atom(lists:concat([T,omitone(kvs:id_seq(list_to_atom(lists:concat([T,".tables"]))))])).
last_table(T)  -> list_to_atom(lists:concat([T,omitone(lists:max(proplists:get_value(T,fold_tables(),[1])))])).
fold_tables()  -> lists:foldl(fun(#table{name=X},Acc) ->
                  setkey(kvs:rname(X),1,Acc,{kvs:rname(X),[kvs:nname(X)|proplists:get_value(kvs:rname(X),Acc,[])]}) end,
                  [], kvs:tables()).
range(L,R,Name) -> #block{left=L,right=R,name=Name,last=last_table(rname(Name))}.
update_config(Table,Name) ->
    Store = store(Table,case kvs:get(config,Table)  of
                                            {error,not_found}        -> update_list(Table,[],Name);
                                            {ok,#config{value=List}} -> update_list(Table,List,Name) end),
%    {M,F} = application:get_env(kvs,forbidding,{?MODULE,forbid}),
    New = Store, %lists:sublist(Store,M:F(Table)),
    kvs:put(#config{key = Table, value = New }).

update_list(Table,[],Name)                    -> [ range(top(Table)+1,limit(),Name) ];
update_list(Table,[#block{}=CI|Tail],Name)    -> [ range(top(Table)+1,limit(),Name) ] ++
                                                 [ CI#block{right=top(Table)}       ] ++ Tail.

setkey(Name,Pos,List,New) ->
    case lists:keyfind(Name,Pos,List) of
        false -> [New|List];
        _Element -> lists:keyreplace(Name,Pos,List,New) end.

test() -> test(#user{}).

test(Proto) ->
    kvs:join(),
    Table = element(1,Proto),
    [ kvs:add(setelement(2,Proto,kvs:next_id(Table,1))) || _ <- lists:seq(1,20000) ],
    io:format("Config: ~p~n",[kvs:all(config)]),
    io:format("Fetch: ~p~n",[kvs:entries(kvs:get(feed,Table),Table,10)]).
