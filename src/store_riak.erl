-module(store_riak).
-author('Maxim Sokhatsky <maxim@synrc.com>').
-copyright('Synrc Research Center s.r.o.').
-include_lib("kvs/include/config.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/acls.hrl").
-include_lib("kvs/include/invites.hrl").
-include_lib("kvs/include/meetings.hrl").
-include_lib("kvs/include/membership.hrl").
-include_lib("kvs/include/payments.hrl").
-include_lib("kvs/include/purchases.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/log.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

start() -> ok.
stop() -> ok.
version() -> {version,"KVS RIAK 1.3.2-voxoz"}.

initialize() ->
    C = riak:client_connect(node()),
    ets:new(config, [named_table,{keypos,#config.key}]),
    ets:insert(config, #config{ key = "riak_client", value = C}),
    ok.

dir() ->
    C = riak_client(),
    {ok,Buckets} = C:list_buckets(),
    [{table,binary_to_list(X)}||X<-Buckets].

riak_clean(Table) when is_list(Table)->
    C = riak_client(),
    {ok,Keys}=C:list_keys(erlang:list_to_binary(Table)),
    [ C:delete(erlang:list_to_binary(Table),Key) || Key <- Keys];
riak_clean(Table) ->
    C = riak_client(),
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
    error_logger:info_msg("RIAK PUT IDX ~p",[Indices]),
    Obj2.

make_indices(#subscription{who=Who, whom=Whom}) -> [
    {<<"who_bin">>, key_to_bin(Who)},
    {<<"whom_bin">>, key_to_bin(Whom)}];

make_indices(#group_subscription{who=UId, where=GId}) -> [
    {<<"who_bin">>, key_to_bin(UId)},
    {<<"where_bin">>, key_to_bin(GId)}];

make_indices(#user{username=UId,zone=Zone}) -> [
    {<<"user_bin">>, key_to_bin(UId)},
    {<<"zone_bin">>, key_to_bin(Zone)}];

make_indices(#user_product{username=UId,product_id=PId}) -> [
    {<<"user_bin">>, key_to_bin(UId)},
    {<<"product_bin">>, key_to_bin(PId)}];

make_indices(#payment{id=Id,user_id=UId}) -> [
    {<<"payment_bin">>, key_to_bin(Id)},
    {<<"user_bin">>, key_to_bin(UId)}];

make_indices(#comment{id={CID,EID},author_id=Who}) -> [
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

riak_client() -> [{_,_,{_,C}}] = ets:lookup(config, "riak_client"), C.

put(Records) when is_list(Records) -> lists:foreach(fun riak_put/1, Records);
put(Record) -> riak_put(Record).

riak_put(Record) ->
    Object = make_object(Record),
    Riak = riak_client(),
    Result = Riak:put(Object),
    post_write_hooks(Record, Riak),
    Result.

put_if_none_match(Record) ->
    Object = make_object(Record),
    Riak = riak_client(),
    case Riak:put(Object, [if_none_match]) of
        ok -> post_write_hooks(Record, Riak), ok;
        Error -> Error end.

update(Record, Object) ->
    NewObject = make_object(Record),
    NewKey = riak_object:key(NewObject),
    case riak_object:key(Object) of
        NewKey ->
            MetaInfo = riak_object:get_update_metatdata(NewObject),
            UpdObject2 = riak_object:update_value(Object, Record),
            UpdObject3 = riak_object:update_metadata(UpdObject2, MetaInfo),
            Riak = riak_client(),
            case Riak:put(UpdObject3, [if_not_modified]) of
                ok -> post_write_hooks(Record, Riak), ok;
                Error -> Error
            end;
        _ -> {error, keys_not_equal}
    end.

post_write_hooks(R,C) ->
    case element(1,R) of
        user -> case R#user.email of
                    undefined -> nothing;
                    _ -> C:put(make_object({email, R#user.username, R#user.email})) end,
                case R#user.verification_code of
                    undefined -> nothing;
                    _ -> C:put(make_object({code, R#user.username, R#user.verification_code})) end,
                case R#user.facebook_id of
                  undefined -> nothing;
                  _ -> C:put(make_object({facebook, R#user.username, R#user.facebook_id})) end;
        _ -> continue end.

get(Tab, Key) ->
    Bucket = key_to_bin(Tab),
    IntKey = key_to_bin(Key),
    riak_get(Bucket, IntKey).

riak_get(Bucket,Key) ->
    C = riak_client(),
    RiakAnswer = C:get(Bucket,Key),
    case RiakAnswer of
        {ok, O} -> {ok, riak_object:get_value(O)};
        X -> X end.

get_for_update(Tab, Key) ->
    C = riak_client(),
    case C:get(key_to_bin(Tab), key_to_bin(Key)) of
        {ok, O} -> {ok, riak_object:get_value(O), O};
        Error -> Error end.

delete(Tab, Key) ->
    C = riak_client(),
    Bucket = key_to_bin(Tab),
    IntKey = key_to_bin(Key),
    C:delete(Bucket, IntKey).

delete_by_index(Tab, IndexId, IndexVal) ->
    Riak = riak_client(),
    Bucket = key_to_bin(Tab),
    {ok, Keys} = Riak:get_index(Bucket, {eq, IndexId, key_to_bin(IndexVal)}),
    [Riak:delete(Bucket, Key) || Key <- Keys],
    ok.

key_to_bin(Key) ->
    if is_integer(Key) -> erlang:list_to_binary(integer_to_list(Key));
       is_list(Key) -> erlang:list_to_binary(Key);
       is_atom(Key) -> erlang:list_to_binary(erlang:atom_to_list(Key));
       is_binary(Key) -> Key;
       true ->  [ListKey] = io_lib:format("~p", [Key]), erlang:list_to_binary(ListKey)
    end.

all(RecordName) ->
    Riak = riak_client(),
    RecordBin = key_to_bin(RecordName),
    {ok,Keys} = Riak:list_keys(RecordBin),
    Results = [ riak_get_raw({RecordBin, Key, Riak}) || Key <- Keys ],
    [ Object || Object <- Results, Object =/= failure ].

all_by_index(Tab, IndexId, IndexVal) ->
    Riak = riak_client(),
    Bucket = key_to_bin(Tab),
    {ok, Keys} = Riak:get_index(Bucket, {eq, IndexId, key_to_bin(IndexVal)}),
    F = fun(Key, Acc) ->
                case Riak:get(Bucket, Key, []) of
                    {ok, O} -> [riak_object:get_value(O) | Acc];
                    {error, notfound} -> Acc end end,
    lists:foldl(F, [], Keys).

riak_get_raw({RecordBin, Key, Riak}) ->
    case Riak:get(RecordBin, Key) of
        {ok,O} -> riak_object:get_value(O);
        X -> failure end.

next_id(CounterId) -> next_id(CounterId, 1).
next_id(CounterId, Incr) -> next_id(CounterId, 0, Incr).
next_id(CounterId, Default, Incr) ->
    Riak = riak_client(),
    CounterBin = key_to_bin(CounterId),
    {Object, Value, Options} =
        case Riak:get(key_to_bin(id_seq), CounterBin, []) of
            {ok, CurObj} ->
                R = #id_seq{id = CurVal} = riak_object:get_value(CurObj),
                NewVal = CurVal + Incr,
                Obj = riak_object:update_value(CurObj, R#id_seq{id = NewVal}),
                {Obj, NewVal, [if_not_modified]};
            {error, notfound} ->
                NewVal = Default + Incr,
                Obj = riak_object:new(key_to_bin(id_seq), CounterBin, #id_seq{thing = CounterId, id = NewVal}),
                {Obj, NewVal, [if_none_match]} end,
    case Riak:put(Object, Options) of
        ok -> Value;
        {error, _} -> next_id(CounterId, Incr) end.

% index funs

products(UId) -> all_by_index(user_product, <<"user_bin">>, list_to_binary(UId)).
subscriptions(UId) -> all_by_index(subsciption, <<"subs_who_bin">>, list_to_binary(UId)).
subscribed(Who) -> all_by_index(subscription, <<"subs_whom_bin">>, list_to_binary(Who)).
participate(UserName) -> all_by_index(group_subscription, <<"who_bin">>, UserName).
members(GroupName) -> all_by_index(group_subscription, <<"where_bin">>, GroupName).
user_tournaments(UId) -> all_by_index(play_record, <<"play_record_who_bin">>, list_to_binary(UId)).
tournament_users(TId) -> all_by_index(play_record, <<"play_record_tournament_bin">>, list_to_binary(integer_to_list(TId))).
author_comments(Who) ->
    EIDs = [E || #comment{entry_id=E} <- all_by_index(comment,<<"author_bin">>, Who) ],
    lists:flatten([ all_by_index(entry,<<"entry_bin">>,EID) || EID <- EIDs]).
