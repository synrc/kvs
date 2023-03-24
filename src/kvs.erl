-module(kvs).

-behaviour(application).

-behaviour(supervisor).

-include_lib("stdlib/include/assert.hrl").

-include("api.hrl").

-include("metainfo.hrl").

-include("stream.hrl").

-include("cursors.hrl").

-include("kvs.hrl").

-include("backend.hrl").

-export([dump/0,
         db/0,
         metainfo/0,
         ensure/1,
         ensure/2,
         seq_gen/0,
         keys/1,
         fields/1,
         defined/2,
         field/2,
         setfield/3,
         remove/1]).

-export([join/2, seq/3]).

-export(?API)

.

-export(?STREAM)

.

-export([init/1, start/2, stop/1]).

-record('$msg', {id, next, prev, user, msg}).

init([]) -> {ok, {{one_for_one, 5, 10}, []}}.

start(_, _) ->
    supervisor:start_link({local, kvs}, kvs, []).

stop(_) -> ok.

dba() -> application:get_env(kvs, dba, kvs_mnesia).

seq_dba() -> application:get_env(kvs, dba_seq, kvs_mnesia).

db()  -> (dba()):db().

kvs_stream() ->
    application:get_env(kvs, dba_st, kvs_stream).

% kvs api

all(Table) -> all(Table, #kvs{mod = dba(), db = db()}).

delete(Table, Key) ->
    delete(Table, Key, #kvs{mod = dba(), db = db()}).

get(Table, Key) ->
    (?MODULE):get(Table, Key, #kvs{mod = dba(), db = db()}).

index(Table, K, V) ->
    index(Table, K, V, #kvs{mod = dba()}).

keys(Feed) ->
    keys(Feed, #kvs{mod = dba(), db = db()}).

key_match(Feed, Id) ->
  key_match(Feed, Id, #kvs{mod = dba(), db=db()}).

match(Record) ->
    match(Record, #kvs{mod = dba()}).

index_match(Record, Index) ->
    index_match(Record, Index, #kvs{mod = dba()}).

join() -> join([], #kvs{mod = dba(), db = db()}).

dump() -> dump(#kvs{mod = dba()}).

join(Node) -> join(Node, #kvs{mod = dba(), db = db()}).

leave() -> leave(#kvs{mod = dba(), db = db()}).

destroy() -> destroy(#kvs{mod = dba(), db = db()}).

count(Table) -> count(Table, #kvs{mod = dba()}).

put(Record) -> (?MODULE):put(Record, #kvs{mod = dba(), db = db()}).

stop() -> stop_kvs(#kvs{mod = dba()}).

start() -> start(#kvs{mod = dba()}).

ver() -> ver(#kvs{mod = dba()}).

dir() -> dir(#kvs{mod = dba()}).

feed(Key) ->
    feed(Key, #kvs{mod = dba(), st = kvs_stream(), db = db()}).

seq([], DX)    -> seq([], DX, #kvs{mod = kvs_rocks});
seq(Table, DX) -> seq(Table, DX, #kvs{mod = seq_dba()}).

remove(Rec, Feed) ->
    remove(Rec, Feed, #kvs{mod = dba(), st = kvs_stream(), db = db()}).

put(Records, #kvs{mod = Mod, db = Db}) when is_list(Records) ->
    Mod:put(Records, Db);
put(Record, #kvs{mod = Mod, db = Db}) -> Mod:put(Record, Db).

get(RecordName, Key, #kvs{mod = Mod, db = Db}) ->
    Mod:get(RecordName, Key, Db).

delete(Tab, Key, #kvs{mod = Mod, db = Db}) ->
    Mod:delete(Tab, Key, Db).

delete_range(Feed, Last, #kvs{mod=DBA, db=Db}) ->
    DBA:delete_range(Feed,Last,Db).

count(Tab, #kvs{mod = DBA}) -> DBA:count(Tab).

index(Tab, Key, Value, #kvs{mod = DBA}) ->
    DBA:index(Tab, Key, Value).

keys(Feed, #kvs{mod = DBA, db = Db}) ->
    DBA:keys(Feed, Db).

key_match(Feed, Id, #kvs{mod = DBA, db = Db}) ->
    DBA:key_match(Feed, Id, Db).

match(Record, #kvs{mod = DBA}) ->
    DBA:match(Record).

index_match(Record, Index, #kvs{mod = DBA}) ->
    DBA:index_match(Record, Index).

seq(Tab, Incr, #kvs{mod = DBA}) -> DBA:seq(Tab, Incr).

dump(#kvs{mod = Mod}) -> Mod:dump().

feed(Tab, #kvs{st = Mod, db = Db}) -> Mod:feed(Tab, Db).

remove(Rec, Feed, #kvs{st = Mod, db = Db}) ->
    Mod:remove(Rec, Feed, Db).

all(Tab, #kvs{mod = DBA, db = Db}) -> DBA:all(Tab, Db).

start(#kvs{mod = DBA}) -> DBA:start().

stop_kvs(#kvs{mod = DBA}) -> DBA:stop().

join(Node, #kvs{mod = DBA, db = Db}) -> DBA:join(Node, Db).

leave(#kvs{mod = DBA, db = Db}) -> DBA:leave(Db).

destroy(#kvs{mod = DBA, db = Db}) -> DBA:destroy(Db).

ver(#kvs{mod = DBA}) -> DBA:version().

dir(#kvs{mod = DBA}) -> DBA:dir().

% stream api

top(X) -> (kvs_stream()):top(X).

top(X,#kvs{db = Db}) -> (kvs_stream()):top(X,Db).

bot(X) -> (kvs_stream()):bot(X).

bot(X,#kvs{db = Db})         -> (kvs_stream()):bot(X,Db).

next(X)                      -> (kvs_stream()):next(X).

next(X,#kvs{db = Db})        -> (kvs_stream()):next(X,Db).

prev(X)                      -> (kvs_stream()):prev(X).

prev(X,#kvs{db = Db})        -> (kvs_stream()):prev(X,Db).

drop(X)                      -> (kvs_stream()):drop(X).

drop(X,#kvs{db = Db})        -> (kvs_stream()):drop(X,Db).

take(X)                      -> (kvs_stream()):take(X).

take(X,#kvs{db = Db})        -> (kvs_stream()):take(X,Db).

save(X)                      -> (kvs_stream()):save(X).

save(X,#kvs{db = Db})        -> (kvs_stream()):save(X,Db).

remove(X)                    -> (kvs_stream()):remove(X).

cut(X)                       -> (kvs_stream()):cut(X).

cut(X,#kvs{db = Db})         -> (kvs_stream()):cut(X, Db).

add(X)                       -> (kvs_stream()):add(X).

add(X,#kvs{db = Db})         -> (kvs_stream()):add(X,Db).

append(X, Y)                 -> (kvs_stream()):append(X, Y).

append(X, Y, #kvs{db = Db})  -> (kvs_stream()):append(X, Y, Db).

load_reader(X)               -> (kvs_stream()):load_reader(X).

load_reader(X,#kvs{db = Db}) -> (kvs_stream()):load_reader(X,Db).

writer(X)                    -> (kvs_stream()):writer(X).

writer(X,#kvs{db = Db})      -> (kvs_stream()):writer(X,Db).

reader(X)                    -> (kvs_stream()):reader(X).

reader(X,#kvs{db = Db})      -> (kvs_stream()):reader(X,Db).

% unrevisited

ensure(#writer{} = X) -> ensure(X,#kvs{mod = dba(), db = db()}).
ensure(#writer{id = Id},#kvs{} = X) ->
    case kvs:get(writer, Id, X) of
        {error, _} ->
            kvs:save(kvs:writer(Id, X)),
            ok;
        {ok, _} -> ok
    end.

cursors() ->
    lists:flatten([[{T#table.name, T#table.fields}
                    || #table{name = Name} = T
                           <- (M:metainfo())#schema.tables,
                       Name == reader orelse Name == writer]
                   || M <- modules()]).

% metainfo api

tables() ->
    lists:flatten([(M:metainfo())#schema.tables
                   || M <- modules()]).

modules() -> application:get_env(kvs, schema, []).

metainfo() ->
    #schema{name = kvs, tables = core() ++ test_tabs()}.

core() ->
    [#table{name = id_seq,
            fields = record_info(fields, id_seq), keys = [thing]}].

test_tabs() ->
    [#table{name = '$msg',
            fields = record_info(fields, '$msg')}].

table(Name) when is_atom(Name) ->
    lists:keyfind(Name, #table.name, tables());
table(_) -> false.

seq_gen() ->
    Init = fun (Key) ->
                   case kvs:get(id_seq, Key) of
                       {error, _} ->
                           {Key, kvs:put(#id_seq{thing = Key, id = 0})};
                       {ok, _} -> {Key, skip}
                   end
           end,
    [Init(atom_to_list(Name))
     || {Name, _Fields} <- cursors()].

initialize(Backend, Module) ->
    [begin
         Backend:create_table(T#table.name,
                              [{attributes, T#table.fields},
                               {T#table.copy_type, [node()]},
                               {type, T#table.type}]),
         [Backend:add_table_index(T#table.name, Key)
          || Key <- T#table.keys],
         T
     end
     || T <- (Module:metainfo())#schema.tables].

fields(Table) when is_atom(Table) ->
    case table(Table) of
        false -> [];
        T -> T#table.fields
    end.

defined(TableRecord, Field) ->
    FieldsList = fields(element(1, TableRecord)),
    lists:member(Field, FieldsList).

field(TableRecord, Field) ->
    FieldsList = fields(element(1, TableRecord)),
    Index = string:str(FieldsList, [Field]) + 1,
    element(Index, TableRecord).

setfield(TableRecord, Field, Value) ->
    FieldsList = fields(element(1, TableRecord)),
    Index = string:str(FieldsList, [Field]) + 1,
    setelement(Index, TableRecord, Value).
