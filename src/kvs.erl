-module(kvs).
-copyright('Synrc Research Center s.r.o.').
-compile(export_all).
-include_lib("stdlib/include/qlc.hrl").
-include("config.hrl").
-include("metainfo.hrl").
-include("kvs.hrl").
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
add(Table)         -> add     (Table, #kvs{mod=?DBA}).
all(Table)         -> all     (Table, #kvs{mod=?DBA}).
put(Table)         -> put     (Table, #kvs{mod=?DBA}).
link(Table)        -> link    (Table, #kvs{mod=?DBA}).
traversal(T,S,C,D) -> traversal(T,S,C,D, #kvs{mod=?DBA}).
info(T)            -> info    (T,        #kvs{mod=?DBA}).
start()            -> start   (#kvs{mod=?DBA}).
stop()             -> stop    (#kvs{mod=?DBA}).
destroy()          -> destroy (#kvs{mod=?DBA}).
version()          -> version (#kvs{mod=?DBA}).
dir()              -> dir     (#kvs{mod=?DBA}).
next_id(Table,DX)  -> next_id(Table, DX,  #kvs{mod=?DBA}).

generation(Table,Key) ->
    case Key - topleft(Table,Key) < application:get_env(kvs,generation,250000) of
         true -> skip;
         false -> kvs:rotate(Table) end.

% Implementation

init(Backend, Module) ->
    [ begin
        Backend:create_table(T#table.name, [{attributes,T#table.fields},{T#table.copy_type, [node()]}]),
        [ Backend:add_table_index(T#table.name, Key) || Key <- T#table.keys ],
        T
    end || T <- (Module:metainfo())#schema.tables ].

start(#kvs{mod=DBA}) -> DBA:start().
stop(#kvs{mod=DBA}) -> DBA:stop().
change_storage(Type) -> [ change_storage(Name,Type) || #table{name=Name} <- kvs:tables() ].
change_storage(Table,Type,#kvs{mod=DBA}) -> DBA:change_storage(Table,Type).
destroy(#kvs{mod=DBA}) -> DBA:destroy().
join(Node,#kvs{mod=DBA}) -> DBA:join(Node), rotate_new(), load_partitions().
version(#kvs{mod=DBA}) -> DBA:version().
tables() -> lists:flatten([ (M:metainfo())#schema.tables || M <- modules() ]).
table(Name) -> lists:keyfind(Name,#table.name,tables()).
dir(#kvs{mod=DBA}) -> DBA:dir().
info(T,#kvs{mod=DBA}) -> DBA:info(T).
modules() -> kvs:config(schema).
containers() ->
    lists:flatten([ [ {T#table.name,T#table.fields}
        || T=#table{container=true} <- (M:metainfo())#schema.tables ]
    || M <- modules() ]).

create(ContainerName) -> create(ContainerName, kvs:next_id(atom_to_list(ContainerName), 1), #kvs{mod=?DBA}).

create(ContainerName, Id, Driver) ->
    kvs:info(?MODULE,"Create: ~p",[ContainerName]),
    Instance = list_to_tuple([ContainerName|proplists:get_value(ContainerName, kvs:containers())]),
    Top  = setelement(#container.id,Instance,Id),
    Top2 = setelement(#container.top,Top,undefined),
    Top3 = setelement(#container.count,Top2,0),
    ok = kvs:put(Top3, Driver),
    Id.

ensure_link(Record, #kvs{mod=_Store}=Driver) ->

    Id    = element(2,Record),
    Type  = rname(element(1,Record)),
    CName = element(#iterator.container, Record),
    Cid   = case element(#iterator.feed_id, Record) of
               undefined -> rname(element(1,Record));
                     Fid -> Fid end,

    Container = case kvs:get(CName, Cid, Driver) of
        {ok,Res} -> Res;
        {error, _} when Cid /= undefined ->
                NC = setelement(#container.id,
                      list_to_tuple([CName|
                            proplists:get_value(CName, kvs:containers())]), Cid),
                NC1 = setelement(#container.count, NC, 0),
                NC1;
        _Error -> error end,

    case Container of
              error -> {error, no_container};
        _ when element(#container.top,Container) == Id -> {error,just_added};
                  _ ->
                       Top = case element(#container.top, Container) of
                                   undefined -> undefined;
                                   Tid -> case kvs:get(Type, Tid, Driver) of
                                               {error, _} -> undefined;
                                               {ok, T}  -> setelement(#iterator.next, T, Id) end end,

                       Prev = case Top of undefined -> undefined; E -> element(#iterator.id, E) end,
                       Next = undefined,

                       C1 = setelement(#container.top, Container, Id),
                       C2 = setelement(#container.count, C1,
                               element(#container.count, Container)+1),

                       R  = setelement(#iterator.feeds, Record,
                            [ case F1 of
                                {FN, Fd} -> {FN, Fd};
                                _-> {F1, kvs:create(CName,{F1,element(#iterator.id,Record)},Driver)}
                              end || F1 <- element(#iterator.feeds, Record)]),

                       R1 = setelement(#iterator.next,    R,  Next),
                       R2 = setelement(#iterator.prev,    R1, Prev),
                       R3 = setelement(#iterator.feed_id, R2, element(#container.id, Container)),

                       case {kvs:put(R3, Driver),Top} of            % Iterator
                            {ok,undefined} -> kvs:put(C2, Driver);  % Container
                            {ok,Top}       -> kvs:put(C2, Driver),
                                              kvs:put(Top, Driver);
                                        __ -> kvs:error(?MODULE,"Error Updating Iterator: ~p~n",
                                                                [element(#container.id,R3)]) end,

                       kvs:info(?MODULE,"Put: ~p~n", [element(#container.id,R3)]),

                    {ok, R3}
            end.

link(Record,#kvs{mod=_Store}=Driver) ->
    Id = element(#iterator.id, Record),
    case kvs:get(rname(element(1,Record)), Id, Driver) of
              {ok, Exists} -> ensure_link(Exists, Driver);
        {error, not_found} -> {error, not_found} end.

add(Record, #kvs{mod=store_mnesia}=Driver) when is_tuple(Record) -> store_mnesia:add(Record);
add(Record, #kvs{mod=Store}=Driver) when is_tuple(Record) -> append(Record,Driver).

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

relink(Container, E, Driver) ->
    Id   = element(#iterator.id, E),
    Next = element(#iterator.next, E),
    Prev = element(#iterator.prev, E),
    Top  = element(#container.top, Container),
    case kvs:get(element(1,E), Prev, Driver) of
         {ok, PE} -> kvs:put(setelement(#iterator.next, PE, Next), Driver);
         _ -> ok end,
    case kvs:get(element(1,E), Next, Driver) of
         {ok, NE} -> kvs:put(setelement(#iterator.prev, NE, Prev), Driver);
                _ -> ok end,
    C  = case Top of
               Id -> setelement(#container.top, Container, Prev);
                _ -> Container end,
    case element(#container.top,C) of
         undefined -> kvs:delete(element(1,C),element(#container.id,C));
         _ -> kvs:put(setelement(#container.count,C,element(#container.count,C)-1), Driver) end.

delete(Tab, Key, #kvs{mod=Mod}) ->
    case range(Tab,Key) of
         [] -> Mod:delete(Tab, Key);
          T -> Mod:delete(T, Key) end.

remove(Record,Id,#kvs{mod=Mod}=Driver) ->
    case get(Record,Id) of
         {error, not_found} -> kvs:error(?MODULE,"Can't remove ~p~n",[{Record,Id}]);
                     {ok,R} -> do_remove(R,Driver) end.

do_remove(E,#kvs{mod=Mod}=Driver) ->
    case get(element(#iterator.container,E),element(#iterator.feed_id,E)) of
         {ok, C} -> relink(C,E,Driver);
               _ -> skip end,
    kvs:info(?MODULE,"Delete: ~p", [E]),
    kvs:delete(element(1,E),element(2,E), Driver).

traversal(Table, Start, Count, Direction, Driver)->
    fold(fun(A,Acc) -> [A|Acc] end,[],Table,Start,Count,Direction,Driver).

% kvs:fold(fun(X,A)->[X|A]end,[],process,2152,-1,#iterator.next,#kvs{mod=store_mnesia}).

fold(___,___,_,undefined,_,_,_) -> [];
fold(___,Acc,_,_,0,_,_) -> Acc;
fold(Fun,Acc,Table,Start,Count,Direction,Driver) ->
    %io:format("fold: ~p~n",[{rname(Table), Start, Driver}]),
    case kvs:get(rname(Table), Start, Driver) of
         {ok, R} -> Prev = element(Direction, R),
                    Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
                    fold(Fun, Fun(R,Acc), Table, Prev, Count1, Direction, Driver);
           Error -> %kvs:error(?MODULE,"Error: ~p~n",[Error]),
                    Acc end.

entries({error,_},_,_,_)      -> [];
entries({ok,Container},N,C,Driver) -> entries(Container,N,C,Driver);
entries(T,N,C,Driver)              -> traversal(N,element(#container.top,T),C,#iterator.prev,Driver).
entries(N, Start, Count, Direction, Driver) ->
    E = traversal(N, Start, Count, Direction, Driver),
    case Direction of #iterator.next -> lists:reverse(E);
                      #iterator.prev -> E end.

add_seq_ids() ->
    Init = fun(Key) ->
           case kvs:get(id_seq, Key) of
                {error, _} -> {Key,kvs:put(#id_seq{thing = Key, id = 0})};
                {ok, _} -> {Key,skip} end end,
    [ Init(atom_to_list(Name))  || {Name,_Fields} <- containers() ].

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

count(RecordName,#kvs{mod=DBA}) -> DBA:count(RecordName).
all(RecordName,#kvs{mod=DBA}) -> DBA:all(RecordName).
index(RecordName, Key, Value,#kvs{mod=DBA}) -> DBA:index(RecordName, Key, Value).
next_id(RecordName, Incr,#kvs{mod=DBA}) -> DBA:next_id(RecordName, Incr).

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

dump() ->
     io:format("~20w ~20w ~10w ~10w~n",[name,storage_type,memory,size]),
   [ io:format("~20w ~20w ~10w ~10w~n",[Name,
         mnesia:table_info(Name,storage_type),
         mnesia:table_info(Name,memory),
         mnesia:table_info(Name,size)]) || #table{name=Name} <- kvs:tables()],
     io:format("Snapshot taken: ~p~n",[calendar:now_to_datetime(os:timestamp())]).

                % Table Partitions

range(RecordName,Id)   -> (find(kvs:config(kvs:rname(RecordName)),RecordName,Id))#interval.name.
topleft(RecordName,Id) -> (find(kvs:config(kvs:rname(RecordName)),RecordName,Id))#interval.left.
last(RecordName,Id)    -> (find(kvs:config(kvs:rname(RecordName)),RecordName,Id))#interval.last.

find([],_,_Id) -> #interval{left=1,right=infinity,name=[],last=[]};
find([Range|T],RecordName,Id) ->
     case lookup(Range,Id) of
          [] -> find(T,RecordName,Id);
          Interval -> Interval end.

lookup(#interval{left=Left,right=Right,name=Name}=I,Id) when Id =< Right, Id >= Left -> I;
lookup(#interval{},_) -> [].

rotate_new()       -> N = [ kvs:rotate(kvs:table(T)) || {T,_} <- fold_tables(),
                            length(proplists:get_value(attributes,kvs:info(last_disc(T)),[])) /=
                            length((kvs:table(kvs:last_table(rname(T))))#table.fields)
                         ], io:format("Nonexistent: ~p~n",[N]), N.
rotate(#table{}=T) -> Name = name(rname(T#table.name)),
                      init(setelement(#table.name,kvs:table(kvs:last_table(T#table.name)),Name)),
                      update_config(rname(T#table.name),Name);
rotate(Table)      -> rotate(kvs:table(Table)).
load_partitions()  -> [ case kvs:get(config,Table) of
                             {ok,{config,_,List}} -> application:set_env(kvs,Table,List);
                             Else -> ok end || {table,Table} <- kvs:dir() ].

omitone(1)     -> [];
omitone(X)     -> X.
limit()        -> infinity.
store(Table,X) -> application:set_env(kvs,Table,X), X.
rname(Table)   -> list_to_atom(lists:filter(fun(X) -> not lists:member(X,"1234567890") end, atom_to_list(Table))).
nname(Table)   -> list_to_integer(case lists:filter(fun(X) -> lists:member(X,"1234567890") end, atom_to_list(Table)) of [] -> "1"; E -> E end).
fold(N)        -> kvs:fold(fun(X,A)->[X|A]end,[],process,N,-1,#iterator.next,#kvs{mod=store_mnesia}).
top(Table)     -> id_seq(Table).
name(T)        -> list_to_atom(lists:concat([T,omitone(kvs:next_id(lists:concat([T,".tables"]),1))])).
init(T)        -> store_mnesia:create_table(T#table.name, [{attributes,T#table.fields},{T#table.copy_type, [node()]}]),
                [ store_mnesia:add_table_index(T#table.name, Key) || Key <- T#table.keys ].
id_seq(Tab)    -> T = atom_to_list(Tab), case kvs:get(id_seq,T) of {ok,#id_seq{id=Id}} -> Id; _ -> kvs:next_id(T,1) end.
last_disc(T)   -> list_to_atom(lists:concat([T,omitone(kvs:id_seq(list_to_atom(lists:concat([T,".tables"]))))])).
last_table(T)  -> list_to_atom(lists:concat([T,omitone(lists:max(proplists:get_value(T,fold_tables(),[1])))])).
fold_tables()  -> lists:foldl(fun(#table{name=X},Acc) ->
                  wf:setkey(kvs:rname(X),1,Acc,{kvs:rname(X),[kvs:nname(X)|proplists:get_value(kvs:rname(X),Acc,[])]}) end,
                  [], kvs:tables()).
interval(L,R,Name) -> #interval{left=L,right=R,name=Name,last=last_table(rname(Name))}.
update_config(Table,Name) ->
    kvs:put(#config{key   = Table,
                    value = store(Table,case kvs:get(config,Table)  of
                                            {error,not_found}        -> update_list(Table,[],Name);
                                            {ok,#config{value=List}} -> update_list(Table,List,Name) end)}).

update_list(Table,[],Name)                    -> [ interval(top(Table)+1,limit(),Name) ];
update_list(Table,[#interval{}=CI|Tail],Name) -> [ interval(top(Table)+1,limit(),Name) ] ++
                                                 [ CI#interval{right=top(Table)}       ] ++ Tail.
