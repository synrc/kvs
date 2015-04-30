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
next_id(Table,DX)  -> next_id (Table, DX,  #kvs{mod=?DBA}).
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
start()            -> start   (#kvs{mod=?DBA}).
stop()             -> stop    (#kvs{mod=?DBA}).
destroy()          -> destroy (#kvs{mod=?DBA}).
version()          -> version (#kvs{mod=?DBA}).
dir()              -> dir     (#kvs{mod=?DBA}).

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
join(Node,#kvs{mod=DBA}) -> DBA:join(Node).
version(#kvs{mod=DBA}) -> DBA:version().
tables() -> lists:flatten([ (M:metainfo())#schema.tables || M <- modules() ]).
table(Name) -> lists:keyfind(Name,#table.name,tables()).
dir(#kvs{mod=DBA}) -> DBA:dir().
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
    Type  = table_type(element(1,Record)),
    CName = element(#iterator.container, Record),
    Cid   = table_type(case element(#iterator.feed_id, Record) of
               undefined -> element(1,Record);
                     Fid -> Fid end),

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
                       Next = undefined,
                       Prev = case element(#container.top, Container) of
                                   undefined -> undefined;
                                   Tid -> case kvs:get(Type, Tid, Driver) of
                                              {error, _} -> undefined;
                                                       {ok, Top} ->
                                        NewTop = setelement(#iterator.next, Top, Id),
                                        kvs:put(NewTop, Driver),
                                        element(#iterator.id, NewTop) end end,

                       C1 = setelement(#container.top, Container, Id),
                       C2 = setelement(#container.count, C1,
                                element(#container.count, Container)+1),

                       kvs:put(C2, Driver), % Container

                       R  = setelement(#iterator.feeds, Record,
                            [ case F1 of
                                {FN, Fd} -> {FN, Fd};
                                _-> {F1, kvs:create(CName,{F1,element(#iterator.id,Record)},Driver)}
                              end || F1 <- element(#iterator.feeds, Record)]),

                       R1 = setelement(#iterator.next,    R,  Next),
                       R2 = setelement(#iterator.prev,    R1, Prev),
                       R3 = setelement(#iterator.feed_id, R2, element(#container.id, Container)),

                       kvs:put(R3, Driver), % Iterator

                       kvs:info(?MODULE,"Put: ~p~n", [element(#container.id,R3)]),

                    {ok, R3}
            end.

link(Record,#kvs{mod=_Store}=Driver) ->
    Id = element(#iterator.id, Record),
    case kvs:get(element(1,Record), Id, Driver) of
              {ok, Exists} -> ensure_link(Exists, Driver);
        {error, not_found} -> {error, not_found} end.

add(Record, #kvs{mod=_Store}=Driver) when is_tuple(Record) ->
    Id = element(#iterator.id, Record),
    case kvs:get(element(1,Record), Id, Driver) of
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
    kvs:put(setelement(#container.count,C,element(#container.count,C)-1), Driver).


delete(Tab, Key, #kvs{mod=Mod}) -> Mod:delete(Tab, Key).

remove(Record,Id,#kvs{mod=Mod}=Driver) ->
    case Mod:get(Record,Id) of
         {error, not_found} -> kvs:error("Can't remove ~p~n",[{Record,Id}]);
                     {ok,R} -> do_remove(R,Driver) end.

do_remove(E,#kvs{mod=Mod}=Driver) ->
    case Mod:get(element(#iterator.container,E),element(#iterator.feed_id,E)) of
         {ok, Container} -> relink(Container,E,Driver);
                       _ -> skip end,
    kvs:info(?MODULE,"Delete: ~p", [E]),
    kvs:delete(element(1,E),element(2,E), Driver).

traversal(Table, Start, Count, Direction, Driver)->
    fold(fun(A,Acc) -> [A|Acc] end,[],Table,Start,Count,Direction,Driver).

fold(_Fun,Acc,_,undefined,_,_,_Driver) -> Acc;
fold(_Fun,Acc,_,_,0,_,_Driver) -> Acc;
fold(Fun,Acc,Table,Start,Count,Direction,Driver) ->
    RecordType = table_type(Table),
    case kvs:get(RecordType, Start, Driver) of
         {ok, R} -> Prev = element(Direction, R),
                    Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
                    fold(Fun, Fun(R,Acc), Table, Prev, Count1, Direction, Driver);
           Error -> kvs:error(?MODULE,"Error: ~p~n",[Error]), [] end.

fold(Fun,Acc,_,undefined,_,_,Driver) -> [];
fold(Fun,Acc,_,_,0,_,Driver) -> Acc;
fold(Fun,Acc,Table,Start,Count,Direction,Driver) ->
    RecordType = table_type(Table),
    case kvs:get(RecordType, Start, Driver) of
         {ok, R} -> Prev = element(Direction, R),
                    Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
                    fold(Fun, Fun(R,Acc), RecordType2, Count1, Direction, Driver);
           Error -> kvs:error(?MODULE,"Error: ~p~n",[Error]), [] end.

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



put(Record,#kvs{mod=DBA}) -> DBA:put(Record).

table_type(user2) -> user;
table_type(A) -> A.

range(RecordName,Id) -> Ranges = kvs:config(RecordName), find(Ranges,RecordName,Id).

find([],_,_Id) -> [];
find([Range|T],RecordName,Id) ->
     case lookup(Range,Id) of
          [] -> find(T,RecordName,Id);
          Name -> Name end.

lookup(#interval{left=Left,right=Right,name=Name},Id) when Id =< Right, Id >= Left -> Name;
lookup(#interval{},_Id) -> [].

get(RecordName, Key, #kvs{mod=Mod}) ->
    case range(RecordName,Key) of
         [] -> Mod:get(RecordName, Key);
         Name ->  Mod:get(Name, Key) end.

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
     io:format("Snapshot taken: ~p~n",[calendar:now_to_datetime(now())]).
