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
-export([destroy/0,join/0,join/1,init_db/0,init/2]).              % schema change
-export([modules/0,containers/0,tables/0,table/1,version/0]).     % meta info
-export([create/1,add/1,remove/2,remove/1]).                      % chain ops
-export([put/1,delete/2,next_id/2]).                              % raw ops
-export([get/2,get/3,index/3]).                                   % read ops
-export([load_db/1,save_db/1]).                                   % import/export

start() -> DBA = ?DBA, DBA:start().
stop() -> DBA = ?DBA, DBA:stop().

destroy() -> DBA = ?DBA, DBA:destroy().
join() -> DBA = ?DBA, DBA:join().
join(Node) -> DBA = ?DBA, DBA:join(Node).
init(Backend, Module) ->
    [ begin
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
    wf:info("kvs:create: ~p",[ContainerName]),
    Instance = list_to_tuple([ContainerName|proplists:get_value(ContainerName, kvs:containers())]),
    Top = setelement(#container.id,Instance,Id),
    Top2 = setelement(#container.top,Top,undefined),
    Top3 = setelement(#container.entries_count,Top2,0),
    ok = kvs:put(Top3),
    Id.

add(Record) when is_tuple(Record) ->

    Id = element(#iterator.id, Record),

    case kvs:get(element(1,Record), Id) of
        {error, not_found} ->

            Type = element(1, Record),
            CName = element(#iterator.container, Record),
            Cid = case element(#iterator.feed_id, Record) of
                undefined -> element(1,Record);
                Fid -> Fid end,

            Container = case kvs:get(CName, Cid) of
                {ok,C} -> C;
                {error, not_found} when Cid /= undefined ->

                    NC = setelement(#container.id,
                            list_to_tuple([CName|proplists:get_value(CName, kvs:containers())]), Cid),
                    NC1 = setelement(#container.entries_count, NC, 0),

                    kvs:put(NC1),NC1;

                _ -> error end,

            if  Container == error -> {error, no_container};
                true ->

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
                    C2 = setelement(#container.entries_count, C1,
                            element(#container.entries_count, Container)+1),

                    kvs:put(C2),

                    R  = setelement(#iterator.feeds, Record,
                            [ case F1 of
                                {FN, Fd} -> {FN, Fd};
                                _-> {F1, kvs:create(CName,{F1,element(#iterator.id,Record)})}
                              end || F1 <- element(#iterator.feeds, Record)]),

                    R1 = setelement(#iterator.next,    R,  Next),
                    R2 = setelement(#iterator.prev,    R1, Prev),
                    R3 = setelement(#iterator.feed_id, R2, element(#container.id, Container)),

                    kvs:put(R3),

                    kvs:info(?MODULE,"[kvs] put: ~p~n", [element(#container.id,R3)]),

                    {ok, R3}
            end;
        {ok, _} -> kvs:info(?MODULE,"[kvs] entry exist while put: ~p~n", [Id]), {error, exist} end.

remove(RecordName, RecordId) ->
    case kvs:get(RecordName, RecordId) of
        {error, not_found} -> ksv:error("[kvs] can't remove ~p~n",[{RecordName,RecordId}]);
        {ok, E} ->

            Id = element(#iterator.id, E),
            CName = element(#iterator.container, E),
            Cid = element(#iterator.feed_id, E),

            {ok, Container} = kvs:get(CName, Cid),
            Top = element(#container.top, Container),

            Next = element(#iterator.next, E),
            Prev = element(#iterator.prev, E),

            case kvs:get(RecordName, Next) of
                {ok, NE} ->
                    NewNext = setelement(#iterator.prev, NE, Prev),
                    kvs:put(NewNext);
                    _ -> ok end,

            case kvs:get(RecordName, Prev) of
                {ok, PE} ->
                    NewPrev = setelement(#iterator.next, PE, Next),
                    kvs:put(NewPrev);
                    _ -> ok end,

            C1 = case Top of Id -> setelement(#container.top, Container, Prev); _ -> Container end,
            C2 = setelement(#container.entries_count, C1, element(#container.entries_count, Container)-1),

            kvs:put(C2),

            kvs:info(?MODULE,"[kvs] delete: ~p id: ~p~n", [RecordName, Id]),

            kvs:delete(RecordName, Id) end.

remove(E) when is_tuple(E) ->

    Id    = element(#iterator.id, E),
    CName = element(#iterator.container, E),
    Cid   = element(#iterator.feed_id, E),

    {ok, Container} = kvs:get(CName, Cid),

    Top   = element(#container.top, Container),
    Next  = element(#iterator.next, E),
    Prev  = element(#iterator.prev, E),

    case kvs:get(element(1,E), Next) of
        {ok, NE} ->
            NewNext = setelement(#iterator.prev, NE, Prev),
            kvs:put(NewNext); _ -> ok end,

    case kvs:get(element(1,E), Prev) of
        {ok, PE} ->
            NewPrev = setelement(#iterator.next, PE, Next),
            kvs:put(NewPrev);
        _ -> ok end,

    C1 = case Top of Id -> setelement(#container.top, Container, Prev); _ -> Container end,
    C2 = setelement(#container.entries_count, C1, element(#container.entries_count, Container)-1),

    kvs:put(C2),

    kvs:info(?MODULE,"[kvs] delete: ~p", [Id]),

    kvs:delete(E).

traversal( _,undefined,_,_) -> [];
traversal(_,_,0,_) -> [];
traversal(RecordType, Start, Count, Direction)->
    case kvs:get(RecordType, Start) of {error,_} -> [];
    {ok, R} ->  Prev = element(Direction, R),
                Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
                [R | traversal(RecordType, Prev, Count1, Direction)] end.

entries(Name) -> Table = kvs:table(Name), entries(kvs:get(Table#table.container,Name), Name, undefined).
entries(Name, Count) -> Table = kvs:table(Name), entries(kvs:get(Table#table.container,Name), Name, Count).
entries({ok, Container}, RecordType, Count) -> entries(Container, RecordType, Count);
entries(Container, RecordType, Count) when is_tuple(Container) ->
    traversal(RecordType, element(#container.top, Container), Count, #iterator.prev).

entries(RecordType, Start, Count, Direction) ->
    E = traversal(RecordType, Start, Count, Direction),
    case Direction of #iterator.next -> lists:reverse(E); #iterator.prev -> E end.

init_db() ->
    case kvs:get(id_seq,"feed") of
        {error,_} -> add_seq_ids();
        {ok,_} -> skip end.

add_seq_ids() ->
    Init = fun(Key) ->
           case kvs:get(id_seq, Key) of
                {error, _} -> {Key,kvs:put(#id_seq{thing = Key, id = 0})};
                {ok, _} -> {Key,skip} end end,
    [ Init(atom_to_list(Name))  || {Name,_Fields} <- containers() ].


put(Record) ->
    DBA=?DBA,
    DBA:put(Record).

get(RecordName, Key) ->
    DBA=?DBA,
    DBA:get(RecordName, Key).

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

config(Key) -> config(kvs, Key, "").
config(App,Key) -> config(App,Key, "").
config(App, Key, Default) -> case application:get_env(App,Key) of
                              undefined -> Default;
                              {ok,V} -> V end.

log_modules() -> [].
-define(ALLOWED, (config(kvs,log_modules,kvs))).

log(Module, String, Args, Fun) ->
    case lists:member(Module,?ALLOWED:log_modules()) of
         true -> error_logger:Fun(String, Args);
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

