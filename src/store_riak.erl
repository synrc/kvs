-module(store_riak).
-author('Maxim Sokhatsky <maxim@synrc.com>').
-include("users.hrl").
-include("feeds.hrl").
-include("acls.hrl").
-include("invites.hrl").
-include("attachments.hrl").
-include("meetings.hrl").
-include("membership_packages.hrl").
-include("accounts.hrl").
-include("log.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

-define(BUCKET_INDEX, "bucket_bin").
-define(MD_INDEX, <<"index">>).
-define(COUNTERS_BUCKET_ID_SEQ, <<"id_seq">>).

delete() -> ok.
start() -> ok.
stop() -> stopped.

initialize() ->
    C = riak:client_connect(?RIAKSERVER_NODE),
    ets:new(config, [named_table,{keypos,#config.key}]),
    ets:insert(config, #config{ key = "riak_client", value = C}),
    ok.


init_indexes() ->
    C = riak_client(),
    ok = C:set_bucket(t_to_b(mhits), [{backend, leveldb_backend}]),
    ok = C:set_bucket(t_to_b(affiliates_rels), [{backend, leveldb_backend}]),
    ok = C:set_bucket(t_to_b(affiliates_contracts), [{backend, leveldb_backend}]),
    ok = C:set_bucket(t_to_b(affiliates_purchases), [{backend, leveldb_backend}]),
    ok = C:set_bucket(t_to_b(affiliates_contract_types), [{backend, leveldb_backend}]),
    ok = C:set_bucket(?COUNTERS_BUCKET_ID_SEQ, [{backend, leveldb_backend}]),
    ok = C:set_bucket(t_to_b(subs), [{backend, leveldb_backend}]),
    ok = C:set_bucket(t_to_b(group_subs), [{backend, leveldb_backend}]),
    ok = C:set_bucket(t_to_b(user_bought_gifts), [{backend, leveldb_backend}]),
    ok = C:set_bucket(t_to_b(play_record), [{backend, leveldb_backend}]),
    ok = nsm_gifts_db:init_indexes().

init_db() ->
%    ?INFO("~w:init_db/0: started", [?MODULE]),
    C = riak_client(),
    ok = nsm_affiliates:init_db(),
    ok = nsm_gifts_db:init_db(),
%    ?INFO("~w:init_db/0: done", [?MODULE]),
    ok.

dir() ->
    C = riak_client(),
    {ok,Buckets} = C:list_buckets(),
    [binary_to_list(X)||X<-Buckets].

clean() ->
    riak_clean(mhits),
    riak_clean(affiliates_contracts), riak_clean(affiliates_rels), riak_clean(affiliates_counters), riak_clean(affiliates_purchases), riak_clean(affiliates_contract_types),
    riak_clean(gifts_counters), riak_clean(gifts_config), riak_clean(gifts),
    riak_clean(play_record), riak_clean(player_scoring), riak_clean(scoring_record), riak_clean(personal_score), riak_clean(pointing_rule),
    riak_clean(tournament), riak_clean(team),
    riak_clean(membership_package), riak_clean(user_purchase), riak_clean(membership_purchase),
    riak_clean(group),
    riak_clean(browser_counter),
    riak_clean(invite_code), riak_clean(forget_password),
    riak_clean(user), riak_clean(user_by_email), riak_clean(user_by_facebook_id), riak_clean(user_address),
    riak_clean(uploads),
    riak_clean(acl), riak_clean(acl_entry),
    riak_clean(account), riak_clean(transaction),
    riak_clean(feature),
    riak_clean(table),
    riak_clean(config),
    riak_clean(user_transaction),
    riak_clean(save_game_table),
    riak_clean(save_table),
    riak_clean(game_table),
    riak_clean(entry), riak_clean(likes), riak_clean(one_like), riak_clean(feed), riak_clean(comment),
    riak_clean(group_member), riak_clean(group_member_rev), riak_clean(subscription), riak_clean(subscription_rev),
    riak_clean(subs), riak_clean(group_subs), riak_clean(user_bought_gifts),
    riak_clean(ut_word), riak_clean(ut_translation),
    riak_clean(active_users_top),
    riak_clean(user_counter),
    riak_clean(twitter_oauth),
    riak_clean(facebook_oauth),
    riak_clean(hidden_feed),
    riak_clean("unsuported"),
    riak_clean("__riak_client_test__"),
    riak_clean(id_seq),
    C=riak_client(),
    C:list_buckets().

riak_clean(Table) when is_list(Table)->
    C = riak_client(),
    {ok,Keys}=C:list_keys(erlang:list_to_binary(Table)),
    [ C:delete(erlang:list_to_binary(Table),Key) || Key <- Keys];
riak_clean(Table) ->
    C = riak_client(),
    [TableStr] = io_lib:format("~p",[Table]),
    {ok,Keys}=C:list_keys(erlang:list_to_binary(TableStr)),
    [ store:delete(Table,key_to_bin(Key)) || Key <- Keys].

% Convert Erlang records to Riak objects
make_object(T, Class) ->
    Obj1 = make_obj(T, Class),
    Bucket = r_to_b(T),
    Indices = [{?BUCKET_INDEX, Bucket} | make_indices(T)], %% Usefull only for level_db buckets
    Meta = dict:store(?MD_INDEX, Indices, dict:new()),
    Obj2 = riak_object:update_metadata(Obj1, Meta),
    Obj2.

%% Record to bucket id
r_to_b(R) -> t_to_b(element(1,R)).

%% Table id to bucket id
t_to_b(T) -> 
    %% list_to_binary(atom_to_list(T)). FIXME: This should be after we eleminate {transaction,_} tables
                                                % no. We use complex keys for leveldb now
    [StringBucket] = io_lib:format("~p",[T]),
    erlang:list_to_binary(StringBucket).

%% Create indicies for the record
make_indices(#mhits{word = Word, ip = IP, date = Date}) -> [{<<"mhits_date_bin">>, key_to_bin(Date)},
                                                            {<<"mhits_word_date_bin">>, key_to_bin({Word, Date})},
                                                            {<<"mhits_word_ip_date_bin">>, key_to_bin({Word,IP,Date})},
                                                            {<<"mhits_word_bin">>, key_to_bin(Word)},
                                                            {<<"mhits_word_ip_bin">>, key_to_bin({Word,IP})},
                                                            {<<"mhits_ip_bin">>, key_to_bin(IP)},
                                                            {<<"mhits_ip_date_bin">>, key_to_bin({IP, Date})}];
make_indices(#affiliates_contracts{owner = OwnerId}) -> [{<<"owner_bin">>, key_to_bin(OwnerId)}];
make_indices(#affiliates_purchases{owner_id = OwnerId, contract_id = ContractId}) -> [{<<"owner_bin">>, key_to_bin(OwnerId)},
                                                                                      {<<"contract_bin">>, key_to_bin(ContractId)}];
make_indices(#affiliates_rels{user = OwnerId, affiliate = OwnerId}) -> [{<<"owner_bin">>, key_to_bin(OwnerId)},
                                                                        {<<"affiliate_bin">>, key_to_bin(1)}];
make_indices(#affiliates_rels{affiliate = OwnerId}) -> [{<<"owner_bin">>, key_to_bin(OwnerId)}];
make_indices(#subs{who=Who, whom=Whom}) -> [{<<"subs_who_bin">>, key_to_bin(Who)},
                                            {<<"subs_whom_bin">>, key_to_bin(Whom)}];
make_indices(#group_subs{user_id=UId, group_id=GId}) -> [{<<"group_subs_user_id_bin">>, key_to_bin(UId)},
                                            {<<"group_subs_group_id_bin">>, key_to_bin(GId)}];
make_indices(#play_record{who=UId, tournament=TId, team=TEId}) -> [{<<"play_record_who_bin">>, key_to_bin(UId)},
                                            {<<"play_record_tournament_bin">>, key_to_bin(TId)}];
make_indices(#user_bought_gifts{username=UId}) -> [{<<"user_bought_gifts_username_bin">>, key_to_bin(UId)}];

make_indices(_Record) -> [].


make_obj(#mhits{word=W, ip=IP, date=D}=T, mhits) -> riak_object:new(r_to_b(T), key_to_bin({W,IP,D}), T);
make_obj(T, affiliates_contract_types) -> riak_object:new(r_to_b(T), key_to_bin(T#affiliates_contract_types.id), T);
make_obj(T, affiliates_contracts) -> riak_object:new(r_to_b(T), key_to_bin(T#affiliates_contracts.id), T);
make_obj(T, affiliates_look_perms) -> riak_object:new(r_to_b(T), key_to_bin(T#affiliates_look_perms.user_id), T);
make_obj(T, affiliates_purchases) -> #affiliates_purchases{contract_id = ContractId, user_id = UserId} = T,
                                     riak_object:new(r_to_b(T), key_to_bin({ContractId, UserId}), T);
make_obj(T, affiliates_rels) -> riak_object:new(r_to_b(T), key_to_bin(T#affiliates_rels.user), T);
make_obj(T, subs) -> [Key] = io_lib:format("~p", [{T#subs.who, T#subs.whom}]), riak_object:new(r_to_b(T), list_to_binary(Key), T);
make_obj(T, group_subs) -> [Key] = io_lib:format("~p", [{T#group_subs.user_id, T#group_subs.group_id}]), riak_object:new(r_to_b(T), list_to_binary(Key), T);
make_obj(T, play_record) -> [Key] = io_lib:format("~p", [{T#play_record.who, T#play_record.tournament}]), riak_object:new(r_to_b(T), list_to_binary(Key), T);
make_obj(T, user_bought_gifts) -> [Key] = io_lib:format("~p", [{T#user_bought_gifts.username, T#user_bought_gifts.timestamp}]), riak_object:new(r_to_b(T), list_to_binary(Key), T);
make_obj(T, account) -> [Key] = io_lib:format("~p", [T#account.id]), riak_object:new(<<"account">>, list_to_binary(Key), T);
make_obj(T, feed) -> riak_object:new(<<"feed">>, list_to_binary(integer_to_list(T#feed.id)), T);
make_obj(T, user) -> riak_object:new(<<"user">>, list_to_binary(T#user.username), T);
make_obj(T, user_address) -> riak_object:new(<<"user_address">>, list_to_binary(T#user_address.username), T);
make_obj(T, game_table) -> riak_object:new(<<"game_table">>, list_to_binary(T#game_table.id), T);
make_obj(T, player_scoring) -> riak_object:new(<<"player_scoring">>, list_to_binary(T#player_scoring.id), T);
make_obj(T, scoring_record) -> riak_object:new(<<"scoring_record">>, list_to_binary(integer_to_list(T#scoring_record.id)), T);
make_obj(T, personal_score) -> riak_object:new(<<"personal_score">>, list_to_binary(T#personal_score.uid), T);
make_obj(T, save_game_table) -> riak_object:new(<<"save_game_table">>, list_to_binary(integer_to_list(T#save_game_table.id)), T);
make_obj(T, feature) -> riak_object:new(<<"feature">>, list_to_binary(T#feature.name), T);
make_obj(T, config) -> riak_object:new(<<"config">>, list_to_binary(T#config.key), T);
make_obj(T = {user_by_email, _User, Email}, user_by_email) -> riak_object:new(<<"user_by_email">>, list_to_binary(Email), T);
make_obj(T = {user_by_verification_code, _User, Code}, user_by_verification_code) -> riak_object:new(<<"user_by_verification_code">>, list_to_binary(Code), T);
make_obj(T = {user_by_facebook_id, _User, FB}, user_by_facebook_id) -> riak_object:new(<<"user_by_facebook_id">>, list_to_binary(FB), T);
make_obj(T, forget_password) -> riak_object:new(<<"forget_password">>, list_to_binary(T#forget_password.token), T);
make_obj(T, entry) -> [Key] = io_lib:format("~p", [T#entry.id]), riak_object:new(<<"entry">>, list_to_binary(Key), T);
make_obj(T, comment) -> [Key] = io_lib:format("~p", [T#comment.id]), riak_object:new(<<"comment">>, list_to_binary(Key), T);
make_obj(T = {_,Key,_}, id_seq) -> riak_object:new(<<"id_seq">>, key_to_bin(Key), T);
make_obj(T, group) -> riak_object:new(<<"group">>, list_to_binary(T#group.username), T);
make_obj(T, tournament) -> riak_object:new(<<"tournament">>, list_to_binary(integer_to_list(T#tournament.id)), T);
make_obj(T, team) -> riak_object:new(<<"team">>, list_to_binary(integer_to_list(T#team.id)), T);
make_obj(T, acl) -> [Key] = io_lib:format("~p", [T#acl.id]), riak_object:new(<<"acl">>, list_to_binary(Key), T);
make_obj(T, acl_entry) -> [Key] = io_lib:format("~p", [T#acl_entry.id]), riak_object:new(<<"acl_entry">>, list_to_binary(Key), T);
make_obj(T, group_member) -> riak_object:new(<<"group_member">>, list_to_binary(T#group_member.who), T);
make_obj(T, group_member_rev) -> riak_object:new(<<"group_member_rev">>, list_to_binary(T#group_member_rev.group), T);
make_obj(T, subscription) -> riak_object:new(<<"subscription">>, list_to_binary(T#subscription.who), T);
make_obj(T, subscription_rev) -> riak_object:new(<<"subscription_rev">>, list_to_binary(T#subscription_rev.whom), T);
make_obj(T, user_ignores) -> riak_object:new(<<"user_ignores">>, list_to_binary(T#user_ignores.who), T);
make_obj(T, user_ignores_rev) -> riak_object:new(<<"user_ignores_rev">>, list_to_binary(T#user_ignores_rev.whom), T);
make_obj(T, ut_word) -> riak_object:new(<<"ut_word">>, list_to_binary(T#ut_word.english), T);
make_obj(T, ut_translation) -> {Lang, English} = T#ut_translation.source, riak_object:new(<<"ut_translation">>, list_to_binary(Lang ++ "_" ++ English), T);
make_obj(T, membership_package) -> riak_object:new(<<"membership_package">>, list_to_binary(T#membership_package.id), T);
make_obj(T, membership_purchase) -> riak_object:new(<<"membership_purchase">>, list_to_binary(T#membership_purchase.id), T);
make_obj(T, user_purchase) -> riak_object:new(<<"user_purchase">>, list_to_binary(T#user_purchase.user), T);
make_obj(T, pointing_rule) -> [Key] = io_lib:format("~p", [T#pointing_rule.id]), riak_object:new(<<"pointing_rule">>, list_to_binary(Key), T);
make_obj(T, transaction) -> riak_object:new(<<"transaction">>, list_to_binary(T#transaction.id), T);
make_obj(T, user_transaction) -> riak_object:new(<<"user_transaction">>, key_to_bin(T#user_transaction.user), T);
make_obj(T = {feed_blocked_users, UserId, _BlockedUsers}, feed_blocked_users) -> riak_object:new(<<"feed_blocked_users">>, list_to_binary(UserId), T);
make_obj(T, uploads) -> [Key] = io_lib:format("~p", [T#uploads.key]), riak_object:new(<<"uploads">>, list_to_binary(Key), T);
make_obj(T, invite_code) -> riak_object:new(<<"invite_code">>, list_to_binary(T#invite_code.code), T);
make_obj(T = {_,User,_}, invite_code_by_user) -> riak_object:new(<<"invite_code_by_user">>, list_to_binary(User), T);
make_obj(T, invite_by_issuer) -> riak_object:new(<<"invite_by_issuer">>, list_to_binary(T#invite_by_issuer.user), T);
make_obj(T, entry_likes) -> riak_object:new(<<"entry_likes">>, list_to_binary(T#entry_likes.entry_id), T);
make_obj(T, user_likes) -> riak_object:new(<<"user_likes">>, list_to_binary(T#user_likes.user_id), T);
make_obj(T, one_like) -> riak_object:new(<<"one_like">>, list_to_binary(integer_to_list(T#one_like.id)), T);
make_obj(T, active_users_top) -> riak_object:new(<<"active_users_top">>, list_to_binary(integer_to_list(T#active_users_top.no)), T);
make_obj(T, user_count) -> riak_object:new(<<"user_count">>, <<"user_count">>, T);
make_obj(T, twitter_oauth) -> riak_object:new(<<"twitter_oauth">>, list_to_binary(T#twitter_oauth.user_id), T);
make_obj(T, facebook_oauth) -> riak_object:new(<<"facebook_oauth">>, list_to_binary(T#facebook_oauth.user_id), T);
make_obj(T, hidden_feed) -> riak_object:new(<<"hidden_feed">>, list_to_binary(integer_to_list(T#hidden_feed.id)), T);
make_obj(T, user_etries_count) -> riak_object:new(<<"user_etries_count">>, list_to_binary(T#user_etries_count.user_id), T);
make_obj(T, invitation_tree) ->
    Key = case T#invitation_tree.user of
        ?INVITATION_TREE_ROOT -> [K] = io_lib:format("~p", [T#invitation_tree.user]), K;
        String -> String
    end,
    riak_object:new(<<"invitation_tree">>, list_to_binary(Key), T);
make_obj(T, Unsupported) -> riak_object:new(<<"unsuported">>, term_to_binary(Unsupported), T).


riak_client() -> [{_,_,{_,C}}] = ets:lookup(config, "riak_client"), C.

-spec put(tuple() | [tuple()]) -> ok.
put(Records) when is_list(Records) ->
    lists:foreach(fun riak_put/1, Records);
put(Record) ->
    put([Record]).

riak_put(Record) ->
    Class = element(1,Record),
    Object = make_object(Record, Class),
    Riak = riak_client(),
    Result = Riak:put(Object, [{allow_mult,false},{last_write_wins,true}]),
    post_write_hooks(Class, Record, Riak),
    Result.

put_if_none_match(Record) ->
    Class = element(1,Record),
    Object = make_object(Record, Class),
    Riak = riak_client(),
    case Riak:put(Object, [if_none_match]) of
        ok ->
            post_write_hooks(Class, Record, Riak),
            ok;
        Error ->
            Error
    end.

%% update(Record, Meta) -> ok | {error, Reason}
update(Record, Object) ->
    Class = element(1,Record),
    %% Create pseudo new object and take its metadata to store in the updated object
    NewObject = make_object(Record, Class),
    NewKey = riak_object:key(NewObject),
    case riak_object:key(Object) of
        NewKey ->
            MetaInfo = riak_object:get_update_metatdata(NewObject),
            UpdObject2 = riak_object:update_value(Object, Record),
            UpdObject3 = riak_object:update_metadata(UpdObject2, MetaInfo),
            Riak = riak_client(),
            case Riak:put(UpdObject3, [if_not_modified]) of
                ok ->
                    post_write_hooks(Class, Record, Riak),
                    ok;
                Error ->
                    Error
            end;
        _ ->
            {error, keys_not_equal}
    end.


post_write_hooks(Class,R,C) ->
    case Class of
        user -> case R#user.email of
                    undefined -> nothing;
                    _ -> C:put(make_object({user_by_email, R#user.username, R#user.email}, user_by_email))
                end,
                case R#user.verification_code of
                    undefined -> nothing;
                    _ -> C:put(make_object({user_by_verification_code, R#user.username, R#user.verification_code}, user_by_verification_code))
                end,
                case R#user.facebook_id of
                  undefined -> nothing;
                  _ -> C:put(make_object({user_by_facebook_id, R#user.username, R#user.facebook_id}, user_by_facebook_id))
                end;

        invite_code ->
            #invite_code{created_user=User, issuer = Issuer} = R,

            if Issuer =/= undefined,
               User =/= undefined ->
                   nsm_affiliates:invitation_hook(Issuer, User);
               true -> do_nothing
            end,

            case R#invite_code.created_user of
                undefined -> nothing;
                User -> C:put(make_object({invite_code_by_user, User, R#invite_code.code}, invite_code_by_user))
            end;

        _ -> continue
    end.

% get

-spec get(atom(), term()) -> {ok, tuple()} | {error, not_found | duplicated}.
get(Tab, Key) ->
    Bucket = t_to_b(Tab),
    IntKey = key_to_bin(Key),
    riak_get(Bucket, IntKey).

riak_get(Bucket,Key) -> %% TODO: add state monad here for conflict resolution when not last_win strategy used
    C = riak_client(),
    RiakAnswer = C:get(Bucket,Key,[{last_write_wins,true},{allow_mult,false}]),
    case RiakAnswer of
        {ok, O} -> {ok,riak_object:get_value(O)};
        X -> X
    end.

%% get_for_update(Tab, Key) -> {ok, Record, Meta} | {error, Reason}
get_for_update(Tab, Key) ->
    C = riak_client(),
    case C:get(t_to_b(Tab), key_to_bin(Key), [{last_write_wins,true},{allow_mult,false}]) of
        {ok, O} -> {ok, riak_object:get_value(O), O};
        Error -> Error
    end.

% translations

get_word(Word) ->
    get(ut_word,Word).

get_translation({Lang, Word}) ->
    get(ut_translation, Lang ++ "_" ++ Word).

% delete

-spec delete(tuple() | [tuple()]) -> ok.
delete(Keys) when is_list(Keys) ->
    lists:foreach(fun mnesia:delete_object/1, Keys); % TODO
delete(Keys) ->
    delete([Keys]).

-spec delete(atom(), term()) -> ok.
delete(Tab, Key) ->
    C = riak_client(),
    Bucket = t_to_b(Tab),
    IntKey = key_to_bin(Key),
    C:delete(Bucket, IntKey),
    ok.

-spec delete_by_index(atom(), binary(), term()) -> ok.
delete_by_index(Tab, IndexId, IndexVal) ->
    Riak = riak_client(),
    Bucket = t_to_b(Tab),
    {ok, Keys} = Riak:get_index(Bucket, {eq, IndexId, key_to_bin(IndexVal)}),
    [Riak:delete(Bucket, Key) || Key <- Keys],
    ok.

key_to_bin(Key) ->
    if is_integer(Key) -> erlang:list_to_binary(integer_to_list(Key));
       is_list(Key) -> erlang:list_to_binary(Key);
       is_tuple(Key) -> [ListKey] = io_lib:format("~p", [Key]), erlang:list_to_binary(ListKey);
       is_atom(Key) -> erlang:list_to_binary(erlang:atom_to_list(Key));
       is_binary(Key) -> Key;
       true ->  [ListKey] = io_lib:format("~p", [Key]), erlang:list_to_binary(ListKey)
    end.

% search

-spec multi_select(atom(), [term()]) -> [tuple()].
multi_select(_RecordName, _Keys) when is_list(_Keys) -> erlang:error(notimpl).
%    [mnesia:read({RecordName, Key}) || Key <- Keys].

select(RecordName, Pred) when is_function(Pred) ->
	%% FIXME: bruteforce select
	All = all(RecordName),
	lists:filter(Pred, All);

select(RecordName, Select) when is_list(Select) ->
	%% FIXME: dummy select!
	Where = proplists:get_value(where, Select, fun(_)->true end),
	{Position, _Order} = proplists:get_value(order, Select, {1, descending}),

	Limit = proplists:get_value(limit, Select, all),

	Selected = select(RecordName, Where),
	Sorted = lists:keysort(Position, Selected),

	case Limit of
		all ->
			Sorted;
		{Offset, Amoumt} ->
			lists:sublist(Sorted, Offset, Amoumt)
	end.


-spec count(atom()) -> non_neg_integer().
count(_RecordName) ->
    erlang:length(all(_RecordName)).
%   erlang:error(notimpl).
%   mnesia:table_info(RecordName, size). % TODO

-spec all(atom()) -> [tuple()].
all(RecordName) ->
    Riak = riak_client(),
    [RecordStr] = io_lib:format("~p",[RecordName]),
    RecordBin = erlang:list_to_binary(RecordStr),
    {ok,Keys} = Riak:list_keys(RecordBin),
    Results = [ get_record_from_table({RecordBin, Key, Riak}) || Key <- Keys ],
    [ Object || Object <- Results, Object =/= failure ].

%% all_by_index(Tab, IndexId, IndexVal) -> RecordsList
all_by_index(Tab, IndexId, IndexVal) ->
    Riak = riak_client(),
    Bucket = t_to_b(Tab),
    {ok, Keys} = Riak:get_index(Bucket, {eq, IndexId, key_to_bin(IndexVal)}),
    F = fun(Key, Acc) ->
                case Riak:get(Bucket, Key, []) of
                    {ok, O} -> [riak_object:get_value(O) | Acc];
                    {error, notfound} -> Acc
                end
        end,
    lists:foldl(F, [], Keys).

get_record_from_table({RecordBin, Key, Riak}) ->
    case Riak:get(RecordBin, Key) of
        {ok,O} -> riak_object:get_value(O);
        X -> failure
    end.

% id generator

-spec next_id(list()) -> pos_integer().
next_id(CounterId) ->
    next_id(CounterId, 1).

-spec next_id(term(), integer()) -> pos_integer().
next_id(CounterId, Incr) ->
    next_id(CounterId, 0, Incr).

-spec next_id(term(), integer(), integer()) -> pos_integer().
%% Safe implementation of counters
next_id(CounterId, Default, Incr) ->
    Riak = riak_client(),
    CounterBin = key_to_bin(CounterId),
    {Object, Value, Options} =
        case Riak:get(?COUNTERS_BUCKET_ID_SEQ, CounterBin, []) of
            {ok, CurObj} ->
                R = #id_seq{id = CurVal} = riak_object:get_value(CurObj),
                NewVal = CurVal + Incr,
                Obj = riak_object:update_value(CurObj, R#id_seq{id = NewVal}),
                {Obj, NewVal, [if_not_modified]};
            {error, notfound} ->
                NewVal = Default + Incr,
                Obj = riak_object:new(?COUNTERS_BUCKET_ID_SEQ, CounterBin, #id_seq{thing = CounterId, id = NewVal}),
                {Obj, NewVal, [if_none_match]}
        end,
    case Riak:put(Object, Options) of
        ok ->
            Value;
        {error, _} -> %% FIXME: Right reason(s) should be specified here
            next_id(CounterId, Incr)
    end.

% browser counters

-spec delete_browser_counter_older_than(pos_integer()) -> ok.
delete_browser_counter_older_than(_MinTS) -> % TODO
    [].
%    MatchHead = #browser_counter{minute='$1', _ = '_'},
%    Guard = {'<', '$1', MinTS},
%    Result = '$_',
%    List = mnesia:select(browser_counter, [{MatchHead, [Guard], [Result]}]),
%          lists:foreach(fun(X) ->
%                    mnesia:delete_object(X)
%                end, List),
%    List.

unused_invites() -> % TODO
    List = store:all(invite_code),
    length([ #invite_code{created_user=undefined} || _Code <- List]).

user_by_verification_code(Code) ->
    R = case store:get(user_by_verification_code,Code) of
	{ok,{_,User,_}} -> store:get(user,User);
	Else -> Else
	end,
    R.

user_by_facebook_id(FBId) ->
    R = case store:get(user_by_facebook_id,FBId) of
	{ok,{_,User,_}} -> store:get(user,User);
	Else -> Else
	end,
    R.

user_by_email(FB) ->
    R = case store:get(user_by_email,FB) of
	{ok,{_,User,_}} -> store:get(user,User);
	Else -> Else
	end,
    R.

user_by_username(Name) ->
    case X = store:get(user,Name) of
	{ok,_Res} -> X;
	Else -> Else
    end.

%invite_code_by_issuer(User) ->
%    case store:get(invite_code_by_issuer, User) of
%        {ok, {invite_code_by_user, _, Code}} ->
%            case store:get(invite_code, Code) of
%                {ok, #invite_code{} = C} ->
%                    [C];
%                _ ->
%                    []
%            end;
%        _ ->
%            []
%    end.

invite_code_by_user(User) ->
    case store:get(invite_code_by_user, User) of
        {ok, {invite_code_by_user, _, Code}} ->
            case store:get(invite_code, Code) of
                {ok, #invite_code{} = C} ->
                    [C];
                _ ->
                    []
            end;
        _ ->
            []
    end.


% Todo: run in background
update_user_name(UId,UName,Surname) ->
    Name = case {UName,Surname} of
        {undefined,undefined} -> UId;
        _ -> io_lib:format("~s ~s", [UName, Surname])
    end,
    case store:get(group_member, UId) of
    {error, notfound} -> ok;
    {ok, #group_member{group=List}} ->
        UpdateUserName = fun(#group_member{group=GId,type=Type}, _) ->
            case store:get(group_member_rev, GId) of
            {error, notfound} ->
                store:put(#group_member_rev{group=GId,
                                                who=[#group_member_rev{who=UId,who_name=Name,group=GId,type=Type}],
                                                type=list});
            {ok,#group_member_rev{who=Whos}} ->
                NewWhos = lists:map(fun(#group_member_rev{who=U}=M) when U==UId->
                                           M#group_member_rev{who_name=Name};(M)->M end, Whos),
                store:put(#group_member_rev{group=GId, who=NewWhos, type=list})
            end
        end,
        lists:foldl(UpdateUserName, undefined, List)
    end.

% game info

get_save_tables(Id) ->
    case store:get(save_game_table, Id) of
	{error,notfound} -> [];
	{ok,R} -> R
    end.

save_game_table_by_id(Id) -> store:get(save_game_table, Id).

% subscriptions

subscribe_user(MeId, FrId) ->
    MeShow = case store:get(user, MeId) of
        {ok, #user{name=MeName,surname=MeSur}} ->
            io_lib:format("~s ~s", [MeName,MeSur]);
        _Z ->
            io:format("Get ~s: ~p~n", [MeId, _Z]),
            undefined
    end,
    FrShow = case store:get(user, FrId) of
        {ok, #user{name=FrName,surname=FrSur}} ->
            io_lib:format("~s ~s", [FrName,FrSur]);
        _ ->
            undefined
    end,
    Rec = #subscription{who = MeId, whom = FrId, whom_name = FrShow},
    case store:get(subscription, MeId) of
        {error, notfound} ->
            store:put(#subscription{who = MeId, whom = [Rec]});
        {ok, #subscription{whom = List}} ->
            case lists:member(Rec, List) of
                false ->
                    NewList =
                        lists:keystore(FrId, #subscription.whom, List, Rec),
                    store:put(#subscription{who = MeId, whom = NewList});
                true ->
                    do_nothing
            end
    end,
    RevRec = #subscription_rev{whom=FrId, who=MeId, who_name=MeShow},
    case store:get(subscription_rev, FrId) of
        {error,notfound} ->
            store:put(#subscription_rev{whom=FrId, who=[RevRec]});
        {ok,#subscription_rev{who=RevList}} ->
            case lists:member(RevRec, RevList) of
                false ->
                    NewRevList =
                        lists:keystore(MeId, #subscription_rev.who, RevList, RevRec),
                    store:put(#subscription_rev{whom=FrId, who=NewRevList});
                true ->
                    do_nothing
            end
    end,
    ok.

remove_subscription(MeId, FrId) ->
    List = nsm_users:list_subscription(MeId),
    Subs = [ Sub || Sub <- List, not(Sub#subscription.who==MeId andalso Sub#subscription.whom==FrId) ],
    store:put(#subscription{who = MeId, whom = Subs}),
    List2 = nsm_users:list_subscription_me(FrId),
    Revs = [ Rev || Rev <- List2, not(Rev#subscription_rev.who==MeId andalso Rev#subscription_rev.whom==FrId) ],
    store:put(#subscription_rev{who = Revs, whom = FrId}).

list_subscriptions(#user{username = UId}) -> list_subscriptions(UId);
list_subscriptions(UId) when is_list(UId) ->
    case store:get(subscription, UId) of
	{ok,#subscription{whom = C}} -> C;
	_ -> []
    end.

list_subscription_me(UId) ->
    case store:get(subscription_rev, UId) of
	{ok,#subscription_rev{who = C}} -> C;
	_ -> []
    end.

is_user_subscribed(Who,Whom) ->
    case store:get(subscription, Who) of
    {ok,#subscription{whom = W}} ->
        lists:any(fun(#subscription{who=Who1, whom=Whom1}) -> Who1==Who andalso Whom1==Whom; (_)->false end, W);
    _ -> false
    end.


% blocking user

block_user(Who, Whom) ->
    case store:get(user_ignores, Who) of
        {error, notfound} ->
            store:put(#user_ignores{who = Who, whom = [Whom]});
        {ok, #user_ignores{whom = List}} ->
            case lists:member(Whom, List) of
                false ->
                    NewList = [Whom | List],
                    store:put(#user_ignores{who = Who, whom = NewList});
                true ->
                    do_nothing
            end
    end,
    case store:get(user_ignores_rev, Whom) of
        {error,notfound} ->
            store:put(#user_ignores_rev{whom=Whom, who=[Who]});
        {ok,#user_ignores_rev{who=RevList}} ->
            case lists:member(Who, RevList) of
                false ->
                    NewRevList = [Who | RevList],
                    store:put(#user_ignores_rev{whom=Whom, who=NewRevList});
                true ->
                    do_nothing
            end
    end,
    ok.

unblock_user(Who, Whom) ->
    List = list_blocks(Who),
    case lists:member(Whom, List) of
        true ->
            NewList = [ UID || UID <- List, UID =/= Whom ],
            store:put(#user_ignores{who = Who, whom = NewList});
        false ->
            do_nothing
    end,
    List2 = list_blocked_me(Whom),
    case lists:member(Who, List2) of
        true ->
            NewRevList = [ UID || UID <- List2, UID =/= Who ],
            store:put(#user_ignores_rev{whom = Whom, who = NewRevList});
        false ->
            do_nothing
    end.

list_blocks(#user{username = UId}) -> list_blocks(UId);
list_blocks(UId) when is_list(UId) ->
    case store:get(user_ignores, UId) of
        {ok,#user_ignores{whom = C}} -> C;
        _ -> []
    end.

list_blocked_me(UId) ->
    case store:get(user_ignores_rev, UId) of
        {ok,#user_ignores_rev{who = C}} -> C;
        _ -> []
    end.

is_user_blocked(Who, Whom) ->
    case store:get(user_ignores, Who) of
        {ok,#user_ignores{whom = List}} ->
            lists:member(Whom, List);
        _ -> false
    end.

% feeds

feed_add_direct_message(FId,User,To,EntryId,Desc,Medias) -> feed_add_entry(FId,User,To,EntryId,Desc,Medias,{user,direct},"").
feed_add_entry(FId,From,EntryId,Desc,Medias) -> feed_add_entry(FId,From,undefined,EntryId,Desc,Medias,{user,normal},"").
feed_add_entry(FId, User, To, EntryId,Desc,Medias,Type,SharedBy) ->
    %% prevent adding of duplicate records to feed
    case store:entry_by_id({EntryId, FId}) of
        {ok, _} -> ok;
        _ -> do_feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy)
    end.

do_feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy) ->
    {ok,Feed} = store:get(feed,erlang:integer_to_list(FId)),
    Id = {EntryId, FId},
    Next = undefined,
    Prev = case Feed#feed.top of
               undefined ->
                   undefined;
               X ->
                   case store:get(entry, X) of
                       {ok, TopEntry} ->
                           EditedEntry = TopEntry#entry{next = Id},
                           % update prev entry
                           store:put(EditedEntry),
                           TopEntry#entry.id;
                       {error,notfound} ->
                           undefined
                   end
           end,

    store:put(#feed{id = FId, top = {EntryId, FId}}), % update feed top with current

    Entry  = #entry{id = {EntryId, FId},
                    entry_id = EntryId,
                    feed_id = FId,
                    from = User,
                    to = To,
                    type = Type,
                    media = Medias,
                    created_time = now(),
                    description = Desc,
                    raw_description = Desc,
                    shared = SharedBy,
                    next = Next,
                    prev = Prev},

    ModEntry = case catch feedformat:format(Entry) of
                   {_, Reason} ->
                       ?ERROR("feedformat error: ~p", [Reason]),
                       Entry;
                   #entry{} = ME ->
                       ME
               end,

    store:put(ModEntry),
    {ok, ModEntry}.

feed_add_comment(FId, User, EntryId, ParentComment, CommentId, Content, Medias) ->
    FullId = {CommentId, {EntryId, FId}},

    Prev = case ParentComment of
        undefined ->
            {ok, Entry} = store:entry_by_id({EntryId, FId}),
            {PrevC, E} = case Entry#entry.comments of
                        undefined ->
                            {undefined, Entry#entry{comments_rear = FullId}};
                        Id ->
                            {ok, PrevTop} = store:get(comment, Id),
                            store:put(PrevTop#comment{next = FullId}),
                            {Id, Entry}
                   end,

            store:put(E#entry{comments=FullId}),
            PrevC;

        _ ->
            {ok, Parent} = store:get(comment, {{EntryId, FId}, ParentComment}),
            {PrevC, CC} = case Parent#comment.comments of
                        undefined ->
                            {undefined, Parent#comment{comments_rear = FullId}};
                        Id ->
                            {ok, PrevTop} = store:get(comment, Id),
                            store:put(PrevTop#comment{next = FullId}),
                            {Id, Parent}
                    end,
            store:put(CC#comment{comments = FullId}),
            PrevC
    end,
    Comment = #comment{id = FullId,
                       author_id = User,
                       comment_id = CommentId,
                       entry_id = EntryId,
                       raw_content = Content,
                       content = Content,
                       media = Medias,
                       create_time = now(),
                       prev = Prev,
                       next = undefined
                      },
    store:put(Comment),
    {ok, Comment}.

add_transaction_to_user(UserId,Purchase) ->
    {ok,Team} = case store:get(user_transaction, UserId) of
                     {ok,T} -> {ok,T};
                     _ -> ?INFO("user_transaction not found"),
                          Head = #user_transaction{ user = UserId, top = undefined},
                          {store:put(Head),Head}
                end,

    EntryId = Purchase#transaction.id, %store:next_id("membership_purchase",1),
    Prev = undefined,
    case Team#user_transaction.top of
        undefined -> Next = undefined;
        X -> case store:get(transaction, X) of
                 {ok, TopEntry} ->
                     Next = TopEntry#transaction.id,
                     EditedEntry = #transaction {
                           commit_time = TopEntry#transaction.commit_time,
                           amount = TopEntry#transaction.amount,
                           remitter = TopEntry#transaction.remitter,
                           acceptor = TopEntry#transaction.acceptor,
                           currency = TopEntry#transaction.currency,
                           info = TopEntry#transaction.info,
                           id = TopEntry#transaction.id,
                           next = TopEntry#transaction.next,
                           prev = EntryId },
                    store:put(EditedEntry); % update prev entry
                 {error,notfound} -> Next = undefined
             end
    end,

    Entry  = #transaction{id = EntryId,
                           commit_time = Purchase#transaction.commit_time,
                           amount = Purchase#transaction.amount,
                           remitter = Purchase#transaction.remitter,
                           acceptor = Purchase#transaction.acceptor,
                           currency = Purchase#transaction.currency,
                           info = Purchase#transaction.info,
                           next = Next,
                           prev = Prev},

    case store:put(Entry) of ok -> store:put(#user_transaction{ user = UserId, top = EntryId}), {ok, EntryId};
                              Error -> ?INFO("Cant write transaction"), {failure,Error} end.

add_purchase_to_user(UserId,Purchase) ->
    {ok,Team} = case store:get(user_purchase, UserId) of
                     {ok,T} -> ?INFO("user_purchase found"), {ok,T};
                     _ -> ?INFO("user_purchase not found"),
                          Head = #user_purchase{ user = UserId, top = undefined},
                          {store:put(Head),Head}
                end,

    EntryId = Purchase#membership_purchase.id, %store:next_id("membership_purchase",1),
    Prev = undefined,
    case Team#user_purchase.top of
        undefined -> Next = undefined;
        X -> case store:get(membership_purchase, X) of
                 {ok, TopEntry} ->
                     Next = TopEntry#membership_purchase.id,
                     EditedEntry = #membership_purchase{
                           external_id = TopEntry#membership_purchase.external_id,
                           user_id = TopEntry#membership_purchase.user_id,
                           state = TopEntry#membership_purchase.state,
                           membership_package = TopEntry#membership_purchase.membership_package,
                           start_time = TopEntry#membership_purchase.start_time,
                           end_time = TopEntry#membership_purchase.end_time,
                           state_log = TopEntry#membership_purchase.state_log,
                           info = TopEntry#membership_purchase.info,
                           id = TopEntry#membership_purchase.id,
                           next = TopEntry#membership_purchase.next,
                           prev = EntryId},
                    store:put(EditedEntry); % update prev entry
                 {error,notfound} -> Next = undefined
             end
    end,

    store:put(#user_purchase{ user = UserId, top = EntryId}), % update team top with current

    Entry  = #membership_purchase{id = EntryId,
                                  user_id = UserId,
                                  external_id = Purchase#membership_purchase.external_id,
                                  state = Purchase#membership_purchase.state,
                                  membership_package = Purchase#membership_purchase.membership_package,
                                  start_time = Purchase#membership_purchase.start_time,
                                  end_time = Purchase#membership_purchase.end_time,
                                  state_log = Purchase#membership_purchase.state_log,
                                  info = Purchase#membership_purchase.info,
                                  next = Next,
                                  prev = Prev},

    case store:put(Entry) of ok -> {ok, EntryId};
                           Error -> ?INFO("Cant write purchase"), {failure,Error} end.

invite_code_by_issuer(UserId) -> invite_code_by_issuer(UserId, undefined, 1000).

invite_code_by_issuer(UserId, undefined, PageAmount) ->
    case store:get(invite_by_issuer, UserId) of
        {ok, O} when O#invite_by_issuer.top =/= undefined -> 
                                invite_code_by_issuer(UserId, O#invite_by_issuer.top, PageAmount);
        {error, notfound} -> []
    end;
invite_code_by_issuer(UserId, StartFrom, Limit) ->
    case store:get(invite_code,StartFrom) of
        {ok, #invite_code{next = N}=P} -> [ P | riak_traversal(invite_code, #invite_code.next, N, Limit)];
        X -> []
    end.

add_invite_to_issuer(UserId,O) ->
    {ok,Team} = case store:get(invite_by_issuer, UserId) of
                     {ok,T} -> ?INFO("user inviters root found"), {ok,T};
                     _ -> ?INFO("user invites root not found"),
                          Head = #invite_by_issuer{ user = UserId, top = undefined},
                          {store:put(Head),Head}
                end,

    EntryId = O#invite_code.code, 
    Prev = undefined,
    case Team#invite_by_issuer.top of
        undefined -> Next = undefined;
        X -> case store:get(invite_code, X) of
                 {ok, TopEntry} ->
                     Next = TopEntry#invite_code.code,
                     EditedEntry = #invite_code{
                           code = TopEntry#invite_code.code,
                           create_date = TopEntry#invite_code.create_date,
                           issuer = TopEntry#invite_code.issuer,
                           recipient = TopEntry#invite_code.recipient,
                           created_user = TopEntry#invite_code.created_user,
                           next = TopEntry#invite_code.next,
                           prev = EntryId},
                    store:put(EditedEntry); % update prev entry
                 {error,notfound} -> Next = undefined
             end
    end,

    store:put(#invite_by_issuer{ user = UserId, top = EntryId}), % update team top with current

    Entry  = #invite_code{ code = EntryId,
                           create_date = O#invite_code.create_date,
                           issuer = O#invite_code.issuer,
                           recipient = O#invite_code.recipient,
                           created_user = O#invite_code.created_user,
                     next = Next,
                     prev = Prev},

    case store:put(Entry) of ok -> {ok, EntryId};
                           Error -> ?INFO("Cant write invite_by_issuer"), {failure,Error} end.

acl_add_entry(Resource, Accessor, Action) ->
    Acl = case store:get(acl, Resource) of
              {ok, A} ->
                  A;
              %% if acl record wasn't created already
              {error, notfound} ->
                  A = #acl{id = Resource, resource=Resource},
                  store:put(A),
                  A
          end,

    EntryId = {Accessor, Resource},

    case store:get(acl_entry, EntryId) of
        %% there is no entries for specified Acl and Accessor, we have to add it
        {error, notfound} ->
            Next = undefined,
            Prev = case Acl#acl.top of
                       undefined ->
                           undefined;

                       Top ->
                           case store:get(acl_entry, Top) of
                               {ok, TopEntry} ->
                                   EditedEntry = TopEntry#acl_entry{next = EntryId},
                                   store:put(EditedEntry), % update prev entry
                                   TopEntry#acl_entry.id;

                               {error, notfound} ->
                                   undefined
                           end
                   end,

            %% update acl with top of acl entries list
            store:put(Acl#acl{top = EntryId}),

            Entry  = #acl_entry{id = EntryId,
                                entry_id = EntryId,
                                accessor = Accessor,
                                action = Action,
                                next = Next,
                                prev = Prev},

            ok = store:put(Entry),
            Entry;

        %% if acl entry for Accessor and Acl is defined - just change action
        {ok, AclEntry} ->
            store:put(AclEntry#acl_entry{action = Action}),
            AclEntry
    end.


join_tournament(UserId, TournamentId) ->
    case store:get(user, UserId) of
        {ok, User} ->
            GP = case nsm_accounts:balance(UserId, ?CURRENCY_GAME_POINTS) of
                     {ok, AS1} -> AS1;
                     {error, _} -> 0 end,
            K = case nsm_accounts:balance(UserId,  ?CURRENCY_KAKUSH) of
                     {ok, AS2} -> AS2;
                     {error, _} -> 0 end,
            KC = case nsm_accounts:balance(UserId,  ?CURRENCY_KAKUSH_CURRENCY) of
                     {ok, AS3} -> AS3;
                     {error, _} -> 0 end,
            Q = case nsm_accounts:balance(UserId,  ?CURRENCY_QUOTA) of
                     {ok, AS4} -> AS4;
                     {error, _} -> 0 end,
            RN = nsm_users:user_realname(UserId),
            store:put(#play_record{
                 who = UserId,
                 tournament = TournamentId,
                 team = User#user.team,
                 game_id = undefined, 
		 other = now(),
                 realname = RN,
                 game_points = GP,
                 kakush = K,
                 kakush_currency = KC,
                 quota = Q});
        _ ->
            ?INFO(" User ~p not found for joining tournament ~p", [UserId, TournamentId])
    end.

leave_tournament(UserId, TournamentId) ->
    case store:get(play_record, {UserId, TournamentId}) of
        {ok, _} -> 
            store:delete(play_record, {UserId, TournamentId}),
            leave_tournament(UserId, TournamentId); % due to WTF error with old records
        _ -> ok
    end.

user_tournaments(UId) -> 
    store:all_by_index(play_record, <<"play_record_who_bin">>, list_to_binary(UId)).

tournament_waiting_queue(TId) ->
    store:all_by_index(play_record, <<"play_record_tournament_bin">>, list_to_binary(integer_to_list(TId))).


-spec entry_by_id(term()) -> {ok, #entry{}} | {error, not_found}.
entry_by_id(EntryId) -> store:get(entry, EntryId).

-spec comment_by_id({{EntryId::term(), FeedId::term()}, CommentId::term()}) -> {ok, #comment{}}.
comment_by_id(CommentId) -> store:get(CommentId).

-spec comments_by_entry(EId::{string(), term()}) -> [#comment{}].
comments_by_entry({EId, FId}) ->
    case store:entry_by_id({EId, FId}) of
        {ok, #entry{comments_rear = undefined}} ->
            [];
        {ok, #entry{comments_rear = First}} ->
            lists:flatten(read_comments_rev(First));
        _ ->
            []
    end.

purchases(UserId) -> purchases_in_basket(UserId, undefined, 1000).

get_purchases_by_user(UserId, Count, States) -> get_purchases_by_user(UserId, undefined, Count, States).
get_purchases_by_user(UserId, Start, Count, States) ->
    List = purchases_in_basket(UserId, Start, Count),
    case States == all of
        true -> List;
        false -> [P||P<-List, lists:member(P#membership_purchase.state, States)]
    end.

purchases_in_basket(UserId, undefined, PageAmount) ->
    case store:get(user_purchase, UserId) of
        {ok, O} when O#user_purchase.top =/= undefined -> 
                                     purchases_in_basket(UserId, O#user_purchase.top, PageAmount);
        {error, notfound} -> []
    end;
purchases_in_basket(UserId, StartFrom, Limit) ->
    case store:get(membership_purchase,StartFrom) of
        {ok, #membership_purchase{next = N}=P} -> [ P | riak_traversal(membership_purchase, #membership_purchase.next, N, Limit)];
        X -> []
    end.

transactions(UserId) -> tx_list(UserId, undefined, 10000).

tx_list(UserId, undefined, PageAmount) ->
    case store:get(user_transaction, UserId) of
        {ok, O} when O#user_transaction.top =/= undefined -> tx_list(UserId, O#user_transaction.top, PageAmount);
        {error, notfound} -> []
    end;
tx_list(UserId, StartFrom, Limit) ->
    case store:get(transaction,StartFrom) of
        {ok, #transaction{next = N}=P} -> [ P | riak_traversal(transaction, #transaction.next, N, Limit)];
        X -> []
    end.

read_comments(undefined) -> [];
read_comments([#comment{comments = C} | Rest]) -> [read_comments(C) | read_comments(Rest)];
read_comments(C) -> riak_traversal(comment, #comment.prev, C, all).

read_comments_rev(undefined) -> [];
read_comments_rev([#comment{comments = C} | Rest]) -> [read_comments_rev(C) | read_comments_rev(Rest)];
read_comments_rev(C) -> riak_traversal(comment, #comment.next, C, all).

riak_traversal( _, _, undefined, _) -> [];
riak_traversal(_, _, _, 0) -> [];
riak_traversal(RecordType, PrevPos, Next, Count)->
    case nsm_riak:get(RecordType, Next) of
        {error,notfound} -> [];
        {ok, R} ->
            Prev = element(PrevPos, R),
            Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
            [R | riak_traversal(RecordType, PrevPos, Prev, Count1)]
    end.

riak_read_acl_entries(_, undefined, Result) -> Result;
riak_read_acl_entries(C, Next, Result) ->
    NextStr = io_lib:format("~p",[Next]),
    RA = C:get(<<"acl_entry">>,erlang:list_to_binary(NextStr)),
    case RA of
         {ok,RO} -> O = riak_object:get_value(RO), riak_read_acl_entries(C, O#acl_entry.prev, Result ++ [O]);
         {error,notfound} -> Result
    end.

purge_feed(FeedId) ->
    {ok,Feed} = store:get(feed,FeedId),
    Removal = riak_entry_traversal(Feed#feed.top, -1),
    [store:delete(entry,Id)||#entry{id=Id}<-Removal],
    store:put(Feed#feed{top=undefined}).

purge_unverified_feeds() ->
    [purge_feed(FeedId) || #user{feed=FeedId,status=S,email=E} <- store:all(user),E==undefined].

purge_system_messages() ->
    [delete_system_messages(U,FeedId) || U=#user{feed=FeedId} <- store:all(user)].

delete_system_messages(U,FeedId) ->
    ?INFO("Cleaning ~p user feed from system messages",[U]),
    {ok,Feed} = store:get(feed,FeedId),
    Entries = riak_entry_traversal(Feed#feed.top, -1),
    {Removal,Relink} = lists:partition(fun(#entry{type={_,Type}}) -> Type == system orelse Type == system_note end,Entries),
    [store:delete(entry,Id)||#entry{id=Id}<-Removal],
    Len = length(Relink),
    case Len of
         0 -> skip;
         _ ->  Relinked = [begin E = lists:nth(N,Relink),
                               {Next,Prev} = case N of 
                                   1 -> case Len of
                                             1 -> {undefined,undefined};
                                             _ -> NextEntry = lists:nth(N+1,Relink),
                                                 {undefined,NextEntry#entry.id}
                                        end;
                                 Len -> PrevEntry = lists:nth(N-1,Relink),
                                       {PrevEntry#entry.id,undefined};
                                   _ -> PrevEntry = lists:nth(N-1,Relink),
                                        NextEntry = lists:nth(N+1,Relink),
                                       {PrevEntry#entry.id,NextEntry#entry.id}
                                end,
                                E#entry{next = Next, prev = Prev}
               end || N <- lists:seq(1,Len)],
               Link = hd(Relinked),
               store:put(Feed#feed{top=Link#entry.id}),
               [store:put(X) || X <- Relinked] 
    end,
    Len.

riak_entry_traversal(undefined, _) -> [];
riak_entry_traversal(_, 0) -> [];
riak_entry_traversal(Next, Count)->
    case nsm_riak:get(entry, Next) of
        {error,notfound} -> [];
        {ok, R} ->
            Prev = element(#entry.prev, R),
            Count1 = case Count of 
                C when is_integer(C) -> case R#entry.type of
                    {_, system} -> C;   % temporal entries are entries too, but they shouldn't be counted
                    {_, system_note} -> C;
                    _ -> C - 1
                end;
                _-> Count 
            end,
            [R | riak_entry_traversal(Prev, Count1)]
    end.

entries_in_feed(FeedId, undefined, PageAmount) ->
    case store:get(feed, FeedId) of
        {ok, O} -> riak_entry_traversal(O#feed.top, PageAmount);
        {error, notfound} -> []
    end;
entries_in_feed(FeedId, StartFrom, PageAmount) ->
    %% construct entry unic id
    case store:get(entry,{StartFrom, FeedId}) of
        {ok, #entry{prev = Prev}} -> riak_entry_traversal(Prev, PageAmount);
        _ -> []
    end.

acl_entries(AclId) ->
    C = riak_client(),
    [AclStr] = io_lib:format("~p",[AclId]),
    RA = C:get(<<"acl">>, erlang:list_to_binary(AclStr)),
    case RA of
        {ok,RO} ->
            O = riak_object:get_value(RO),
            riak_read_acl_entries(C, O#acl.top, []);
        {error, notfound} -> []
    end.


feed_direct_messages(_FId, Page, PageAmount, CurrentUser, CurrentFId) ->
    Page, PageAmount, CurrentUser, CurrentFId,
    [].

-spec put_into_invitation_tree(Parent::string()|{root}, User::string(), InviteCode::string()) -> #invitation_tree{}.
put_into_invitation_tree(Parent, User, InviteCode) ->
    URecord =  #invitation_tree{user = User,
                                invite_code = InviteCode,
                                parent = Parent},

    case store:get(invitation_tree, Parent) of
        {ok, #invitation_tree{first_child = TopChild} = P} ->
            store:put(P#invitation_tree{first_child = User}),
            URecord1 = URecord#invitation_tree{next_sibling = TopChild},
            store:put(URecord1),
            URecord1;
        _ ->
            R = case Parent of
                    ?INVITATION_TREE_ROOT ->
                        #invitation_tree{user = ?INVITATION_TREE_ROOT};
                    _ ->
                        put_into_invitation_tree(?INVITATION_TREE_ROOT,
                                                 Parent, undefined)
                end,
            store:put(R#invitation_tree{first_child = User}),
            store:put(URecord),
            URecord
    end.


-spec invitation_tree(StartFrom::string()|{root}, Depth::integer()|all) -> [#invitation_tree{}].
invitation_tree(_, -1) -> [];
invitation_tree(none, _) -> [];
invitation_tree(UserId, Depth) ->
    case store:get(invitation_tree, UserId) of
        {ok, #invitation_tree{} = ITreeU} ->
            FirstChild = ITreeU#invitation_tree.first_child,
            SiblingId = ITreeU#invitation_tree.next_sibling,
            Depth1 = case Depth of
                         all -> Depth;
                         _ -> Depth - 1
                     end,
            [ITreeU#invitation_tree{children=invitation_tree(FirstChild, Depth1)}|
                                       invitation_tree(SiblingId, Depth)];
        {error, _} -> []
    end.

%% @doc delete link to child from children list
invitation_tree_delete_child_link(RootUser, User) ->
    case invitation_tree(RootUser, 1) of
        [#invitation_tree{first_child = User} = Root] ->
            Root1 = Root#invitation_tree{children = []},
            case store:get(invitation_tree, User) of
                {ok, #invitation_tree{next_sibling = none}} ->
                    store:put(Root1#invitation_tree{first_child = none});
                {ok, #invitation_tree{next_sibling = Next}} ->
                    store:put(Root1#invitation_tree{first_child = Next});
                _ -> ok
            end;

        [#invitation_tree{children = Children}] ->
            case
                [E || #invitation_tree{next_sibling = NS, user = U} = E <- Children,
                      NS == User orelse U == User] of

                [Before, Exactly] ->
                    NextSibling =  Exactly#invitation_tree.next_sibling,
                    store:put(Before#invitation_tree{next_sibling = NextSibling,
                                                         children = []});
                _ -> ok
            end;
        _ -> ok
    end.


