-module(store_mongo).
-author("Vitaly Shutko").
-author("Oleg Zinchenko").
-copyright('Synrc Research Center s.r.o.').
-include("metainfo.hrl").
-compile(export_all).
-define(POOL_NAME,mongo_pool).

start() -> ok.
stop() -> stopped.
version() -> {version,"KVS MONGO"}.
join([]) -> connect(),[kvs:init(?MODULE,M) || M <- kvs:modules()],ok;
join(_Node) -> {error,not_implemented}.

connect() ->
  {Conn,Pool} = config(),
  case Pool of none -> connect(Conn); _ -> connect(Pool,Conn) end.
connect(Conn) ->
  Spec = {?POOL_NAME,{gen_server,start_link,[{local,?POOL_NAME},mc_worker,Conn,[]]},
          permanent,5000,worker,[kvs_sup]},
  {ok,_} = supervisor:start_child(kvs_sup,Spec),put(no_pool,true),ok.
connect(Pool,Conn) ->
  Spec = poolboy:child_spec(?POOL_NAME,[{name,{local,?POOL_NAME}},{worker_module,mc_worker}]++Pool,Conn),
  {ok,_} = supervisor:start_child(kvs_sup,Spec),ok.

config() ->
  Config = kvs:config(kvs,mongo),
  {connection,Conn} = proplists:lookup(connection,Config),
  P = proplists:lookup(pool,Config),
  Pool = case P of {pool,Pl} -> Pl; _ -> none end,
  {Conn,Pool}.

transaction(Fun) ->
  case get(no_pool) of true -> Fun(?POOL_NAME); _ -> poolboy:transaction(?POOL_NAME,Fun) end.

dir() ->
  {_,{_,{_,_,_,_,_,Colls}}} = transaction(fun (W) -> mongo:command(W,{<<"listCollections">>,1}) end),
  [{table,binary_to_list(C)} || {_,C,_,_} <- Colls].

destroy() -> transaction(fun (W) -> [mongo:command(W,{<<"drop">>,to_binary(T)}) || {_,T} <- dir()] end).

next_id(_Tab,_Incr) -> mongo_id_server:object_id().

to_binary(V) -> to_binary(V,false).
to_binary(V,ForceList) ->
  if is_integer(V) -> V;
    is_list(V) -> unicode:characters_to_binary(V,utf8,utf8);
    is_atom(V) -> list_to_binary(atom_to_list(V));
    is_pid(V)  -> {pid,list_to_binary(pid_to_list(V))};
    true -> case ForceList of true -> [P] = io_lib:format("~p",[V]),list_to_binary(P); _ -> V end
  end.

make_document(Tab,Key,Values) ->
  Table = kvs:table(Tab),
  list_to_tuple(['_id',Key|list_to_doc(tl(Table#table.fields),Values)]).

list_to_doc([],[]) -> [];
list_to_doc([F|Fields],[V|Values]) ->
  case V of
    undefined -> list_to_doc(Fields,Values);
    _ -> [F,to_binary(V)|list_to_doc(Fields,Values)]
  end.

make_record(Tab,Doc) ->
  Table = kvs:table(Tab),
  DocPropList = doc_to_proplist(tuple_to_list(Doc)),
  list_to_tuple([Tab|[proplists:get_value(F,DocPropList) || F <- Table#table.fields]]).

decode_value(<<"true">>) -> true;
decode_value(<<"false">>) -> false;
decode_value({pid,Pid}) -> list_to_pid(binary_to_list(Pid));
decode_value(V) when is_binary(V) -> unicode:characters_to_list(V,utf8);
decode_value(V) -> V.

doc_to_proplist(Doc) -> doc_to_proplist(Doc,[]).
doc_to_proplist([],Acc) -> Acc;
doc_to_proplist(['_id',V|Doc],Acc) -> doc_to_proplist(Doc,[{id,V}|Acc]);
doc_to_proplist([F,V|Doc],Acc) -> doc_to_proplist(Doc,[{F,decode_value(V)}|Acc]).

get(Tab,Key) ->
  Result = transaction(fun (W) -> mongo:find_one(W,to_binary(Tab),{'_id',Key}) end),
  case Result of {} -> {error,not_found}; {Doc} -> make_record(Tab,Doc) end.

put(Records) when is_list(Records) ->
  try lists:foreach(fun mongo_put/1,Records) catch error:Reason -> {error,Reason} end;
put(Record) -> put([Record]).

mongo_put(Record) ->
  Tab = element(1,Record),
  Key = element(2,Record),
  [_,_|Values] = tuple_to_list(Record),
  transaction(fun (W) -> mongo:insert(W,to_binary(Tab),make_document(Tab,Key,Values)) end).

delete(Tab,Key) ->
  transaction(fun (W) -> mongo:delete_one(W,to_binary(Tab),{'_id',Key}) end),ok.

mongo_find(Tab,Sel) ->
  Cursor = transaction(fun (W) -> mongo:find(W,to_binary(Tab),Sel) end),
  Result = mc_cursor:rest(Cursor),
  mc_cursor:close(Cursor),
  case Result of [] -> []; _ -> [make_record(Tab,Doc) || Doc <- Result] end.

all(Tab) -> mongo_find(Tab,{}).
index(Tab,Key,Value) -> mongo_find(Tab,{to_binary(Key),to_binary(Value)}).
create_table(Tab,_Options) -> transaction(fun (W) -> mongo:command(W,{<<"create">>,to_binary(Tab)}) end).

add_table_index(Tab,Key) ->
  transaction(fun (W) -> mongo:ensure_index(W,to_binary(Tab),{key,{to_binary(Key,true),1}}) end).

count(Tab) -> {_,{_,N}} = transaction(fun (W) -> mongo:command(W,{<<"count">>,to_binary(Tab)}) end),N.
