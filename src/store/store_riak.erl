-module(store_riak).
-author('Maxim Sokhatsky <maxim@synrc.com>').
-copyright('Synrc Research Center s.r.o.').
-include("config.hrl").
-include("user.hrl").
-include("subscription.hrl").
-include("group.hrl").
-include("comment.hrl").
-include("entry.hrl").
-include("feed.hrl").
-include("acl.hrl").
-compile(export_all).

start() -> ok.
stop() -> ok.
version() -> {version,"KVS RIAK 2.0.2"}.
join([]) -> ok;
join(Ring) -> riak_core:join(Ring).
initialize() -> riak:client_connect(node()).

dir() ->
    {ok,C}=riak:local_client(),
    {ok,Buckets} = C:list_buckets(),
    [{table,binary_to_list(X)}||X<-Buckets].

riak_clean(Table) when is_list(Table)->
    {ok,C}=riak:local_client(),
    {ok,Keys}=C:list_keys(erlang:list_to_binary(Table)),
    [ C:delete(erlang:list_to_binary(Table),Key) || Key <- Keys];
riak_clean(Table) ->
    {ok,C}=riak:local_client(),
    [TableStr] = io_lib:format("~p",[Table]),
    {ok,Keys}=C:list_keys(erlang:list_to_binary(TableStr)),
    [ kvs:delete(Table,key_to_bin(Key)) || Key <- Keys].

make_object(T) ->
    Bucket = element(1,T),
    Key = element(2,T),
    Obj1 = riak_object:new(key_to_bin(Bucket), key_to_bin(Key), T),
    Indices = make_indices(T),
    Meta = dict:store(<<"index">>, Indices, dict:new()),
    Obj2 = riak_object:update_metadata(Obj1, Meta),
    kvs:info(?MODULE,"RIAK PUT IDX ~p",[Indices]),
    Obj2.

make_indices(#subscription{who=Who, whom=Whom}) -> [
    {<<"who_bin">>, key_to_bin(Who)},
    {<<"whom_bin">>, key_to_bin(Whom)}];

make_indices(#user{id=UId,zone=Zone}) -> [
    {<<"user_bin">>, key_to_bin(UId)},
    {<<"zone_bin">>, key_to_bin(Zone)}];

make_indices(#feed{id=UId}) -> [
    {<<"feed_bin">>, key_to_bin(UId)}];

make_indices(#comment{id={CID,EID},from=Who}) -> [
    {<<"comment_bin">>, key_to_bin({CID,EID})},
    {<<"author_bin">>, key_to_bin(Who)}];

make_indices(#entry{id={EID,FID},entry_id=EntryId,feed_id=Feed,from=From,to=To}) -> [
    {<<"entry_feed_bin">>, key_to_bin({EID,FID})},
    {<<"entry_bin">>, key_to_bin(EntryId)},
    {<<"from_bin">>, key_to_bin(From)},
    {<<"to_bin">>, key_to_bin(To)},
    {<<"feed_bin">>, key_to_bin(Feed)}];

make_indices(Record) -> [
    {key_to_bin(atom_to_list(element(1,Record))++"_bin"),key_to_bin(element(2,Record))}].

put(Records) when is_list(Records) -> lists:foreach(fun riak_put/1, Records);
put(Record) -> riak_put(Record).

riak_put(Record) ->
    {ok,C}=riak:local_client(),
    Bucket = key_to_bin(element(1,Record)),
    Key = key_to_bin(element(2,Record)),
    Object = make_object(Record),
    RiakAnswer = C:get(Bucket,Key),
    case RiakAnswer of
         {ok,O} ->
              Obj = riak_object:update_value(riak_object:reconcile([O],false), Record),
              C:put(Obj);
         _ -> C:put(Object) end.

get(Tab, Key) ->
    Bucket = key_to_bin(Tab),
    IntKey = key_to_bin(Key),
    riak_get(Bucket, IntKey).

riak_get(Bucket,Key) ->
    {ok,C} = riak:local_client(),
    RiakAnswer = C:get(Bucket,Key),
    case RiakAnswer of
        {ok, O} ->
            % kvs:info(?MODULE,"Value Count: ~p~n",[riak_object:value_count(O)]),
            {ok,riak_object:get_value(riak_object:reconcile([O],false))};
        {error, notfound} -> {error, not_found};
        X -> X end.

delete(Tab, Key) ->
    {ok,C}=riak:local_client(),
    Bucket = key_to_bin(Tab),
    IntKey = key_to_bin(Key),
    C:delete(Bucket, IntKey).

delete_by_index(Tab, IndexId, IndexVal) ->
    {ok,C}=riak:local_client(),
    Bucket = key_to_bin(Tab),
    {ok, Keys} = riak_client:get_index(Bucket, {eq, IndexId, key_to_bin(IndexVal)}),
    [C:delete(Bucket, Key) || Key <- Keys].

key_to_bin(Key) ->
    if is_integer(Key) -> erlang:list_to_binary(integer_to_list(Key));
       is_list(Key) -> erlang:list_to_binary(Key);
       is_atom(Key) -> erlang:list_to_binary(erlang:atom_to_list(Key));
       is_binary(Key) -> Key;
       true ->  [ListKey] = io_lib:format("~p", [Key]), erlang:list_to_binary(ListKey) end.

all(RecordName) ->
    {ok,C}=riak:local_client(),
    RecordBin = key_to_bin(RecordName),
    {ok,Keys} = C:list_keys(RecordBin),
    Results = [ riak_get_raw({RecordBin, Key, C}) || Key <- Keys ],
    [ Object || Object <- Results, Object =/= failure ].

all_by_index(Tab, IndexId, IndexVal) ->
    {ok,C}=riak:local_client(),
    Bucket = key_to_bin(Tab),
    {ok, Keys} = C:get_index(Bucket, {eq, IndexId, key_to_bin(IndexVal)}),
    lists:foldl(fun(Key, Acc) ->
        case C:get(Bucket, Key) of
            {ok, O} -> {ok,riak_object:get_value(riak_object:reconcile([O],false))};
            {error, notfound} -> Acc end end, [], Keys).

riak_get_raw({RecordBin, Key, C}) ->
    case C:get(RecordBin, Key) of
        {ok, O} -> riak_object:get_value(riak_object:reconcile([O],false));
        _ -> failure end.

next_id(CounterId) -> next_id(CounterId, 1).
next_id(CounterId, Incr) -> next_id(CounterId, 0, Incr).
next_id(CounterId, Default, Incr) ->
    {ok,C}=riak:local_client(),
    CounterBin = key_to_bin(CounterId),
    {Object, Value, _Options} =
        case C:get(key_to_bin(id_seq), CounterBin) of
            {ok, CurObj} ->
                R = #id_seq{id = CurVal} = riak_object:get_value(CurObj),
                NewVal = CurVal + Incr,
                Obj = riak_object:update_value(CurObj, R#id_seq{id = NewVal}),
                {Obj, NewVal, [if_not_modified]};
            {error, notfound} ->
                NewVal = Default + Incr,
                Obj = riak_object:new(key_to_bin(id_seq), CounterBin, #id_seq{thing = CounterId, id = NewVal}),
                {Obj, NewVal, [if_none_match]} end,
    case C:put(Object) of
        ok -> Value;
        {error, _} -> next_id(CounterId, Incr) end.

% index funs

subscriptions(UId) -> all_by_index(subsciption, <<"subs_who_bin">>, list_to_binary(UId)).
subscribed(Who) -> all_by_index(subscription, <<"subs_whom_bin">>, list_to_binary(Who)).
author_comments(Who) ->
    EIDs = [E || #comment{entry_id=E} <- all_by_index(comment,<<"author_bin">>, Who) ],
    lists:flatten([ all_by_index(entry,<<"entry_bin">>,EID) || EID <- EIDs]).
