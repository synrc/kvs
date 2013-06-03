-module(store_riak).
-author('Maxim Sokhatsky <maxim@synrc.com>').
-include_lib("kvs/include/config.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/acls.hrl").
-include_lib("kvs/include/invites.hrl").
-include_lib("kvs/include/meetings.hrl").
-include_lib("kvs/include/membership_packages.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/log.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

delete() -> ok.
start() -> ok.
stop() -> stopped.

initialize() ->
    C = riak:client_connect(node()),
    ets:new(config, [named_table,{keypos,#config.key}]),
    ets:insert(config, #config{ key = "riak_client", value = C}),
    ok.

init_indexes() ->
    C = riak_client(),
    C:set_bucket(key_to_bin(id_seq), [{backend, leveldb_backend}]),
    C:set_bucket(key_to_bin(subscription), [{backend, leveldb_backend}]),
    C:set_bucket(key_to_bin(user), [{backend, leveldb_backend}]),
    C:set_bucket(key_to_bin(group), [{backend, leveldb_backend}]),
    C:set_bucket(key_to_bin(translation), [{backend, leveldb_backend}]),
    C:set_bucket(key_to_bin(group_subscription), [{backend, leveldb_backend}]),
    C:set_bucket(key_to_bin(user_bought_gifts), [{backend, leveldb_backend}]),
    C:set_bucket(key_to_bin(play_record), [{backend, leveldb_backend}]),
    ok.

dir() ->
    C = riak_client(),
    {ok,Buckets} = C:list_buckets(),
    [binary_to_list(X)||X<-Buckets].

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

make_indices(#subscription{who=Who, whom=Whom}) -> [{<<"subs_who_bin">>, key_to_bin(Who)}, {<<"subs_whom_bin">>, key_to_bin(Whom)}];
make_indices(#group_subscription{user_id=UId, group_id=GId}) -> [{<<"group_subs_user_bin">>, key_to_bin(UId)}, {<<"group_subs_group_bin">>, key_to_bin(GId)}];
make_indices(#user_bought_gifts{username=UId}) -> [{<<"user_bought_gifts_username_bin">>, key_to_bin(UId)}];
make_indices(#user{username=UId,zone=Zone}) -> [{<<"user_bin">>, key_to_bin(UId)}];
make_indices(Record) -> [{key_to_bin(atom_to_list(element(1,Record))++"_bin"),key_to_bin(element(2,Record))}].

riak_client() -> [{_,_,{_,C}}] = ets:lookup(config, "riak_client"), C.

put(Records) when is_list(Records) -> lists:foreach(fun riak_put/1, Records);
put(Record) -> store_riak:put([Record]).

riak_put(Record) ->
    Object = make_object(Record),
    Riak = riak_client(),
    Result = Riak:put(Object),
    error_logger:info_msg("RIAK PUT RES ~p",[Result]),
    post_write_hooks(Record, Riak),
    Result.

put_if_none_match(Record) ->
    Object = make_object(Record),
    Riak = riak_client(),
    case Riak:put(Object, [if_none_match]) of
        ok ->
            post_write_hooks(Record, Riak),
            ok;
        Error ->
            Error
    end.

%% update(Record, Meta) -> ok | {error, Reason}
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
        _ -> continue
    end.

get(Tab, Key) ->
    Bucket = key_to_bin(Tab),
    IntKey = key_to_bin(Key),
    riak_get(Bucket, IntKey).

riak_get(Bucket,Key) ->
    C = riak_client(),
    RiakAnswer = C:get(Bucket,Key),
    case RiakAnswer of
        {ok, O} -> {ok,riak_object:get_value(O)};
        X -> X
    end.

get_for_update(Tab, Key) ->
    C = riak_client(),
    case C:get(key_to_bin(Tab), key_to_bin(Key), [{last_write_wins,true},{allow_mult,false}]) of
        {ok, O} -> {ok, riak_object:get_value(O), O};
        Error -> Error
    end.

get_word(Word) -> store_riak:get(ut_word,Word).
get_translation({Lang, Word}) -> store_riak:get(ut_translation, Lang ++ "_" ++ Word).

% delete

delete(Keys) when is_list(Keys) -> lists:foreach(fun mnesia:delete_object/1, Keys); % TODO
delete(Keys) -> delete([Keys]).

delete(Tab, Key) ->
    C = riak_client(),
    Bucket = key_to_bin(Tab),
    IntKey = key_to_bin(Key),
    C:delete(Bucket, IntKey),
    ok.

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


count(_RecordName) -> erlang:length(all(_RecordName)).

all(RecordName) ->
    Riak = riak_client(),
    [RecordStr] = io_lib:format("~p",[RecordName]),
    RecordBin = erlang:list_to_binary(RecordStr),
    {ok,Keys} = Riak:list_keys(RecordBin),
    Results = [ get_record_from_table({RecordBin, Key, Riak}) || Key <- Keys ],
    [ Object || Object <- Results, Object =/= failure ].

%% get by index

all_by_index(Tab, IndexId, IndexVal) ->
    Riak = riak_client(),
    Bucket = key_to_bin(Tab),
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

% user backlinks

user_by_verification_code(Code) ->
    case kvs:get(code,Code) of
        {ok,{_,User,_}} -> kvs:get(user,User);
        Else -> Else end.

user_by_facebook_id(FBId) ->
    case kvs:get(facebook,FBId) of
        {ok,{_,User,_}} -> kvs:get(user,User);
        Else -> Else end.

user_by_email(Email) ->
    case kvs:get(email,Email) of
        {ok,{_,User,_}} -> kvs:get(user,User);
        Else -> Else end.

user_by_username(Name) ->
    case X = kvs:get(user,Name) of
        {ok,_Res} -> X;
        Else -> Else end.

% feeds

feed_add_direct_message(FId,User,To,EntryId,Desc,Medias) -> feed_add_entry(FId,User,To,EntryId,Desc,Medias,{user,direct},"").
feed_add_entry(FId,From,EntryId,Desc,Medias) -> feed_add_entry(FId,From,undefined,EntryId,Desc,Medias,{user,normal},"").
feed_add_entry(FId, User, To, EntryId,Desc,Medias,Type,SharedBy) ->
    %% prevent adding of duplicate records to feed
    case kvs:entry_by_id({EntryId, FId}) of
        {ok, _} -> ok;
        _ -> do_feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy)
    end.

do_feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy) ->
    {ok,Feed} = kvs:get(feed,erlang:integer_to_list(FId)),
    Id = {EntryId, FId},
    Next = undefined,
    Prev = case Feed#feed.top of
               undefined ->
                   undefined;
               X ->
                   case kvs:get(entry, X) of
                       {ok, TopEntry} ->
                           EditedEntry = TopEntry#entry{next = Id},
                           % update prev entry
                           kvs:put(EditedEntry),
                           TopEntry#entry.id;
                       {error,notfound} ->
                           undefined
                   end
           end,

    kvs:put(#feed{id = FId, top = {EntryId, FId}}), % update feed top with current

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

    kvs:put(ModEntry),
    {ok, ModEntry}.

feed_add_comment(FId, User, EntryId, ParentComment, CommentId, Content, Medias) ->
    FullId = {CommentId, {EntryId, FId}},

    Prev = case ParentComment of
        undefined ->
            {ok, Entry} = kvs:entry_by_id({EntryId, FId}),
            {PrevC, E} = case Entry#entry.comments of
                        undefined ->
                            {undefined, Entry#entry{comments_rear = FullId}};
                        Id ->
                            {ok, PrevTop} = kvs:get(comment, Id),
                            kvs:put(PrevTop#comment{next = FullId}),
                            {Id, Entry}
                   end,

            kvs:put(E#entry{comments=FullId}),
            PrevC;

        _ ->
            {ok, Parent} = kvs:get(comment, {{EntryId, FId}, ParentComment}),
            {PrevC, CC} = case Parent#comment.comments of
                        undefined ->
                            {undefined, Parent#comment{comments_rear = FullId}};
                        Id ->
                            {ok, PrevTop} = kvs:get(comment, Id),
                            kvs:put(PrevTop#comment{next = FullId}),
                            {Id, Parent}
                    end,
            kvs:put(CC#comment{comments = FullId}),
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
    kvs:put(Comment),
    {ok, Comment}.

add_transaction_to_user(UserId,Purchase) ->
    {ok,Team} = case kvs:get(user_transaction, UserId) of
                     {ok,T} -> {ok,T};
                     _ -> ?INFO("user_transaction not found"),
                          Head = #user_transaction{ user = UserId, top = undefined},
                          {kvs:put(Head),Head}
                end,

    EntryId = Purchase#transaction.id, %kvs:next_id("membership_purchase",1),
    Prev = undefined,
    case Team#user_transaction.top of
        undefined -> Next = undefined;
        X -> case kvs:get(transaction, X) of
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
                    kvs:put(EditedEntry); % update prev entry
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

    case kvs:put(Entry) of ok -> kvs:put(#user_transaction{ user = UserId, top = EntryId}), {ok, EntryId};
                              Error -> ?INFO("Cant write transaction"), {failure,Error} end.

add_purchase_to_user(UserId,Purchase) ->
    {ok,Team} = case kvs:get(user_purchase, UserId) of
                     {ok,T} -> ?INFO("user_purchase found"), {ok,T};
                     _ -> ?INFO("user_purchase not found"),
                          Head = #user_purchase{ user = UserId, top = undefined},
                          {kvs:put(Head),Head}
                end,

    EntryId = Purchase#membership_purchase.id, %kvs:next_id("membership_purchase",1),
    Prev = undefined,
    case Team#user_purchase.top of
        undefined -> Next = undefined;
        X -> case kvs:get(membership_purchase, X) of
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
                    kvs:put(EditedEntry); % update prev entry
                 {error,notfound} -> Next = undefined
             end
    end,

    kvs:put(#user_purchase{ user = UserId, top = EntryId}), % update team top with current

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

    case kvs:put(Entry) of ok -> {ok, EntryId};
                           Error -> ?INFO("Cant write purchase"), {failure,Error} end.

acl_add_entry(Resource, Accessor, Action) ->
    Acl = case kvs:get(acl, Resource) of
              {ok, A} ->
                  A;
              %% if acl record wasn't created already
              {error, notfound} ->
                  A = #acl{id = Resource, resource=Resource},
                  kvs:put(A),
                  A
          end,

    EntryId = {Accessor, Resource},

    case kvs:get(acl_entry, EntryId) of
        %% there is no entries for specified Acl and Accessor, we have to add it
        {error, notfound} ->
            Next = undefined,
            Prev = case Acl#acl.top of
                       undefined ->
                           undefined;

                       Top ->
                           case kvs:get(acl_entry, Top) of
                               {ok, TopEntry} ->
                                   EditedEntry = TopEntry#acl_entry{next = EntryId},
                                   kvs:put(EditedEntry), % update prev entry
                                   TopEntry#acl_entry.id;

                               {error, notfound} ->
                                   undefined
                           end
                   end,

            %% update acl with top of acl entries list
            kvs:put(Acl#acl{top = EntryId}),

            Entry  = #acl_entry{id = EntryId,
                                entry_id = EntryId,
                                accessor = Accessor,
                                action = Action,
                                next = Next,
                                prev = Prev},

            ok = kvs:put(Entry),
            Entry;

        %% if acl entry for Accessor and Acl is defined - just change action
        {ok, AclEntry} ->
            kvs:put(AclEntry#acl_entry{action = Action}),
            AclEntry
    end.


join_tournament(UserId, TournamentId) ->
    case kvs:get(user, UserId) of
        {ok, User} ->
            GP = case accounts:balance(UserId, points) of
                     {ok, AS1} -> AS1;
                     {error, _} -> 0 end,
            Q = case accounts:balance(UserId,  quota) of
                     {ok, AS4} -> AS4;
                     {error, _} -> 0 end,
            RN = kvs_users:user_realname(UserId),
            kvs:put(#play_record{
                 who = UserId,
                 tournament = TournamentId,
                 team = User#user.team,
                 game_id = undefined, 
		 other = now(),
                 realname = RN,
                 points = GP,
                 quota = Q});
        _ ->
            ?INFO(" User ~p not found for joining tournament ~p", [UserId, TournamentId])
    end.

leave_tournament(UserId, TournamentId) ->
    case kvs:get(play_record, {UserId, TournamentId}) of
        {ok, _} -> 
            kvs:delete(play_record, {UserId, TournamentId}),
            leave_tournament(UserId, TournamentId); % due to WTF error with old records
        _ -> ok
    end.

user_tournaments(UId) -> 
    kvs:all_by_index(play_record, <<"play_record_who_bin">>, list_to_binary(UId)).

tournament_waiting_queue(TId) ->
    kvs:all_by_index(play_record, <<"play_record_tournament_bin">>, list_to_binary(integer_to_list(TId))).


-spec entry_by_id(term()) -> {ok, #entry{}} | {error, not_found}.
entry_by_id(EntryId) -> kvs:get(entry, EntryId).

-spec comment_by_id({{EntryId::term(), FeedId::term()}, CommentId::term()}) -> {ok, #comment{}}.
comment_by_id(CommentId) -> kvs:get(CommentId).

-spec comments_by_entry(EId::{string(), term()}) -> [#comment{}].
comments_by_entry({EId, FId}) ->
    case kvs:entry_by_id({EId, FId}) of
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
    case kvs:get(user_purchase, UserId) of
        {ok, O} when O#user_purchase.top =/= undefined -> 
                                     purchases_in_basket(UserId, O#user_purchase.top, PageAmount);
        {error, notfound} -> []
    end;
purchases_in_basket(UserId, StartFrom, Limit) ->
    case kvs:get(membership_purchase,StartFrom) of
        {ok, #membership_purchase{next = N}=P} -> [ P | riak_traversal(membership_purchase, #membership_purchase.next, N, Limit)];
        X -> []
    end.

transactions(UserId) -> tx_list(UserId, undefined, 10000).

tx_list(UserId, undefined, PageAmount) ->
    case kvs:get(user_transaction, UserId) of
        {ok, O} when O#user_transaction.top =/= undefined -> tx_list(UserId, O#user_transaction.top, PageAmount);
        {error, notfound} -> []
    end;
tx_list(UserId, StartFrom, Limit) ->
    case kvs:get(transaction,StartFrom) of
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
    case srore_riak:get(RecordType, Next) of
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
    {ok,Feed} = kvs:get(feed,FeedId),
    Removal = riak_entry_traversal(Feed#feed.top, -1),
    [kvs:delete(entry,Id)||#entry{id=Id}<-Removal],
    kvs:put(Feed#feed{top=undefined}).

purge_unverified_feeds() ->
    [purge_feed(FeedId) || #user{feed=FeedId,status=S,email=E} <- kvs:all(user),E==undefined].

riak_entry_traversal(undefined, _) -> [];
riak_entry_traversal(_, 0) -> [];
riak_entry_traversal(Next, Count)->
    case store_riak:get(entry, Next) of
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
    case kvs:get(feed, FeedId) of
        {ok, O} -> riak_entry_traversal(O#feed.top, PageAmount);
        {error, notfound} -> []
    end;
entries_in_feed(FeedId, StartFrom, PageAmount) ->
    %% construct entry unic id
    case kvs:get(entry,{StartFrom, FeedId}) of
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

