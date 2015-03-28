-module(kvs).
-copyright('Synrc Research Center s.r.o.').
-compile(export_all).

-include("api.hrl").
-include("config.hrl").
-include("metainfo.hrl").
-include("state.hrl").
-include("kvs.hrl").
-include_lib("stdlib/include/qlc.hrl").

% NOTE: API Documentation

-export([start/0,stop/0]).                                        % service
-export([destroy/0,join/0,join/1,init/2]).                        % schema change
-export([modules/0,containers/0,tables/0,table/1,version/0]).     % meta info
-export([create/1,add/1,link/1,remove/2,remove/1]).               % chain ops
-export([put/1,delete/2,next_id/2]).                              % raw ops
-export([get/2,get/3,index/3]).                                   % read ops
-export([load_db/1,save_db/1]).                                   % import/export

start() -> DBA = ?DBA, DBA:start().
stop() -> DBA = ?DBA, DBA:stop().

change_storage(Type) -> [ change_storage(Name,Type) || #table{name=Name} <- kvs:tables() ].
change_storage(Table,Type) -> DBA = ?DBA, DBA:change_storage(Table,Type).
destroy() -> DBA = ?DBA, DBA:destroy().
join() -> DBA = ?DBA, DBA:join().
join(Node) -> DBA = ?DBA, DBA:join(Node).
init(Backend, Module) ->
    [ begin
        %io:format("Creating table: ~p~n",[T]),
        Backend:create_table(T#table.name, [{attributes,T#table.fields},{T#table.copy_type, [node()]}]),
        [ Backend:add_table_index(T#table.name, Key) || Key <- T#table.keys ],
        T
    end || T <- (Module:metainfo())#schema.tables ].

version() -> DBA=?DBA, DBA:version().
tables() -> lists:flatten([ (M:metainfo())#schema.tables || M <- modules() ]).
table(Name) -> lists:keyfind(Name,#table.name,tables()).
dir() -> DBA = ?DBA, DBA:dir().
modules() -> kvs:config(schema).
containers() ->
    lists:flatten([ [ {T#table.name,T#table.fields}
        || T=#table{container=true} <- (M:metainfo())#schema.tables ]
    || M <- modules() ]).

create(ContainerName) -> create(ContainerName, kvs:next_id(atom_to_list(ContainerName), 1)).

create(ContainerName, Id) ->
    kvs:info(?MODULE,"Create: ~p",[ContainerName]),
    Instance = list_to_tuple([ContainerName|proplists:get_value(ContainerName, kvs:containers())]),
    Top  = setelement(#container.id,Instance,Id),
    Top2 = setelement(#container.top,Top,undefined),
    Top3 = setelement(#container.count,Top2,0),
    ok = kvs:put(Top3),
    Id.

ensure_link(Record) ->

    Id    = element(2,Record),
    Type  = table_type(element(1,Record)),
    CName = element(#iterator.container, Record),
    Cid   = table_type(case element(#iterator.feed_id, Record) of
               undefined -> element(1,Record);
                     Fid -> Fid end),

    Container = case kvs:get(CName, Cid) of
        {ok,Res} -> Res;
        {error, not_found} when Cid /= undefined ->
                NC = setelement(#container.id,
                      list_to_tuple([CName|
                            proplists:get_value(CName, kvs:containers())]), Cid),
                NC1 = setelement(#container.count, NC, 0),
                kvs:put(NC1),
                NC1;
        _ -> error end,

    case Container of
              error -> {error, no_container};
        _ when element(#container.top,Container) == Id -> {error,just_added};
                  _ ->
                       Next = undefined,
                       Prev = case element(#container.top, Container) of
                                   undefined -> undefined;
                                   Tid -> case kvs:get(Type, Tid) of
                                              {error, not_found} -> undefined;
                                                       {ok, Top} ->
                                        NewTop = setelement(#iterator.next, Top, Id),
                                        kvs:put(NewTop),
                                        element(#iterator.id, NewTop) end end,

                       C1 = setelement(#container.top, Container, Id),
                       C2 = setelement(#container.count, C1,
                                element(#container.count, Container)+1),

                       kvs:put(C2), % Container

                       R  = setelement(#iterator.feeds, Record,
                            [ case F1 of
                                {FN, Fd} -> {FN, Fd};
                                _-> {F1, kvs:create(CName,{F1,element(#iterator.id,Record)})}
                              end || F1 <- element(#iterator.feeds, Record)]),

                       R1 = setelement(#iterator.next,    R,  Next),
                       R2 = setelement(#iterator.prev,    R1, Prev),
                       R3 = setelement(#iterator.feed_id, R2, element(#container.id, Container)),

                       kvs:put(R3), % Iterator

                       kvs:info(?MODULE,"Put: ~p~n", [element(#container.id,R3)]),

                    {ok, R3}
            end.

link(Record) ->
    Id = element(#iterator.id, Record),
    case kvs:get(element(1,Record), Id) of
              {ok, Exists} -> ensure_link(Exists);
        {error, not_found} -> {error, not_found} end.

add(Record) when is_tuple(Record) ->
    Id = element(#iterator.id, Record),
    case kvs:get(element(1,Record), Id) of
        {error, not_found} -> ensure_link(Record);
         {aborted, Reason} -> {aborted, Reason};
                   {ok, _} -> {error, exist} end.

reverse(#iterator.prev) -> #iterator.next;
reverse(#iterator.next) -> #iterator.prev.

relink(Container, E) ->
    Id   = element(#iterator.id, E),
    Next = element(#iterator.next, E),
    Prev = element(#iterator.prev, E),
    Top  = element(#container.top, Container),
    case kvs:get(element(1,E), Prev) of
         {ok, PE} -> kvs:put(setelement(#iterator.next, PE, Next));
         _ -> ok end,
    case kvs:get(element(1,E), Next) of
         {ok, NE} -> kvs:put(setelement(#iterator.prev, NE, Prev));
                _ -> ok end,
    C  = case Top of
               Id -> setelement(#container.top, Container, Prev);
                _ -> Container end,
    kvs:put(setelement(#container.count,C,element(#container.count,C)-1)).

remove(Record,Id) ->
    case kvs:get(Record,Id) of
         {error, not_found} -> kvs:error("Can't remove ~p~n",[{Record,Id}]);
                     {ok,R} -> remove(R) end.

remove(E) ->
    case kvs:get(element(#iterator.container,E),element(#iterator.feed_id,E)) of
         {ok, Container} -> relink(Container,E);
                       _ -> skip end,
    kvs:info(?MODULE,"Delete: ~p", [E]),
    kvs:delete(element(1,E),element(2,E)).

traversal( _,undefined,_,_) -> [];
traversal(_,_,0,_) -> [];
traversal(RecordType2, Start, Count, Direction)->
    RecordType = table_type(RecordType2),
    case kvs:get(RecordType, Start) of
    {ok, R} ->  Prev = element(Direction, R),
                Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
                [R | traversal(RecordType2, Prev, Count1, Direction)];
    Error ->
     io:format("Error: ~p~n",[Error]),
      [] end.

entries(N)                  -> entries(N, undefined).
entries(N,C)                -> T = kvs:table(N), entries(kvs:get(T#table.container,N), N, C).
entries({error,_},_,_)      -> [];
entries({ok,Container},N,C) -> entries(Container,N,C);
entries(T,N,C)              -> traversal(N,element(#container.top,T),C,#iterator.prev).
entries(N, Start, Count, Direction) ->
    E = traversal(N, Start, Count, Direction),
    case Direction of #iterator.next -> lists:reverse(E);
                      #iterator.prev -> E end.

add_seq_ids() ->
    Init = fun(Key) ->
           case kvs:get(id_seq, Key) of
                {error, _} -> {Key,kvs:put(#id_seq{thing = Key, id = 0})};
                {ok, _} -> {Key,skip} end end,
    [ Init(atom_to_list(Name))  || {Name,_Fields} <- containers() ].


put(Record) ->
    DBA=?DBA,
    DBA:put(Record).

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

get(RecordName, Key) ->
    DBA=?DBA,
    case range(RecordName,Key) of
         [] -> DBA:get(RecordName, Key);
         Name ->  DBA:get(Name, Key) end.

get(RecordName, Key, Default) ->
    DBA=?DBA,
    case DBA:get(RecordName, Key) of
        {ok,{RecordName,Key,Value}} ->
            kvs:info(?MODULE,"[kvs] get config value: ~p~n", [{RecordName, Key, Value}]),
            {ok,Value};
        {error, _B} ->
            kvs:info(?MODULE,"[kvs] new config value: ~p~n", [{RecordName, Key, Default}]),
            DBA:put({RecordName,Key,Default}),
            {ok,Default} end.

delete(Tab, Key) -> DBA=?DBA,DBA:delete(Tab, Key).
count(RecordName) -> DBA=?DBA,DBA:count(RecordName).
all(RecordName) -> DBA=?DBA,DBA:all(RecordName).
index(RecordName, Key, Value) -> DBA=?DBA,DBA:index(RecordName, Key, Value).
next_id(RecordName, Incr) -> DBA=?DBA,DBA:next_id(RecordName, Incr).

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

config(Key) -> config(kvs, Key, "").
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

info(Module,String, Args) ->  log(Module,String, Args, info_msg).
info(String, Args) -> log(?MODULE, String, Args, info_msg).
info(String) -> log(?MODULE, String, [], info_msg).

warning(Module,String, Args) -> log(Module, String, Args, warning_msg).
warning(String, Args) -> log(?MODULE, String, Args, warning_msg).
warning(String) -> log(?MODULE,String, [], warning_msg).

error(Module,String, Args) -> log(Module, String, Args, error_msg).
error(String, Args) -> log(?MODULE, String, Args, error_msg).
error(String) -> log(?MODULE, String, [], error_msg).

dump() ->
     io:format("~20w ~20w ~10w ~10w~n",[name,storage_type,memory,size]),
   [ io:format("~20w ~20w ~10w ~10w~n",[Name,
         mnesia:table_info(Name,storage_type),
         mnesia:table_info(Name,memory),
         mnesia:table_info(Name,size)]) || #table{name=Name} <- kvs:tables()],
     io:format("Snapshot taken: ~p~n",[calendar:now_to_datetime(now())]).
