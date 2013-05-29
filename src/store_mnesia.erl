-module(store_mnesia).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-include("config.hrl").
-include("user.hrl").
-include("feed.hrl").
-include("acl.hrl").
-include("invite.hrl").
-include("attachment.hrl").
-include("table.hrl").
-include("uri_translator.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

start() ->
    mnesia:start(),
    mnesia:change_table_copy_type(schema, node(), disc_copies).

stop() -> mnesia:stop().
initialize() -> mnesia:create_schema([node()]).
delete() -> mnesia:delete_schema([node()]).

init_db() ->
    ok = create_table(acl, record_info(fields, acl), [{storage, permanent}]),
    ok = create_table(comment, record_info(fields, comment), [{storage, permanent}, {type, ordered_set}]),
    ok = add_table_index(comment, entry_id),
    ok = create_table(feed, record_info(fields, feed), [{storage, permanent}]),
    ok = create_table(entry, record_info(fields, entry), [{storage, permanent}, {type, ordered_set}]),
    ok = add_table_index(entry, feed_id),
    ok = add_table_index(entry, entry_id),
    ok = add_table_index(entry, from),
    ok = create_table(forget_password, record_info(fields, forget_password), [{storage, permanent}]),
    ok = create_table(prohibited, record_info(fields, prohibited), [{storage, permanent}, {type, bag}]),
    ok = create_table(group_member, record_info(fields, group_member), [{storage, permanent}, {type, bag}]),
    ok = create_table(group_member_rev, record_info(fields, group_member_rev), [{storage, permanent}, {type, bag}]),
    ok = create_table(group, record_info(fields, group), [{storage, permanent}]),
    ok = add_table_index(group_member, id),
    ok = create_table(invite_code, record_info(fields, invite_code), [{storage, permanent}]),
    ok = add_table_index(invite_code, issuer),
    ok = add_table_index(invite_code, created_user),
    ok = create_table(user, record_info(fields, user), [{storage, permanent}]),
    ok = create_table(user_status, record_info(fields, user_status), [{storage, permanent}]),
    ok = create_table(subscription, record_info(fields, subscription), [{storage, permanent}, {type, bag}]),
    ok = create_table(subscription_rev, record_info(fields, subscription_rev), [{storage, permanent}, {type, bag}]),
    ok = add_table_index(user, verification_code),
    ok = add_table_index(user, facebook_id),
    ok = add_table_index(user, email),
    ok = create_table(uploads, record_info(fields, uploads), [{storage, permanent}, {type, set}]),
    ok = create_table(save_game_table, record_info(fields, save_game_table), [{storage, permanent}, {type, bag}]),
    ok = add_table_index(save_game_table, id),
    ok = create_table(id_seq, record_info(fields, id_seq), [{storage, permanent}]),
    ok = create_table(feature, record_info(fields, feature), [{storage, permanent}]),
    ok = create_table(ut_word, record_info(fields, ut_word), [{storage, permanent}, {type, bag}]),
    ok = create_table(ut_translation, record_info(fields, ut_translation), [{storage, permanent}, {type, ordered_set}]),
    nsm_membership_packages:create_storage(),
    ok.

put(Records) when is_list(Records) -> void(fun() -> lists:foreach(fun mnesia:write/1, Records) end);
put(Record) -> put([Record]).

delete(Keys) when is_list(Keys) -> void(fun() -> lists:foreach(fun mnesia:delete_object/1, Keys) end);
delete(Keys) -> delete([Keys]).
delete(Tab, Key) -> mnesia:transaction(fun()-> mnesia:delete({Tab, Key}) end), ok.

multi_select(RecordName, Keys) when is_list(Keys) -> flatten(fun() -> [mnesia:read({RecordName, Key}) || Key <- Keys] end).

select(From, PredicateFunction) when is_function(PredicateFunction) -> exec(qlc:q([Record || Record <- mnesia:table(From), apply(PredicateFunction, [Record])]));
select(From, [{where, Fn}, {order, {Idx, Order}}]) -> exec(qlc:q([R || R <- qlc:keysort(Idx, mnesia:table(From), [{order, Order}]), apply(Fn, [R])]));
select(From, [{where, Fn}, {order, {Idx, Order}}, {limit, {1, Length}}]) ->
    {atomic, Recs} = mnesia:transaction(fun()->
        QC = qlc:cursor(qlc:q(
            [R || R <- qlc:keysort(Idx, mnesia:table(From), [{order, Order}]), apply(Fn, [R])])),
        Ret = qlc:eval(qlc:next_answers(QC, Length)),
        qlc:delete_cursor(QC),
        Ret
    end),
    Recs;

select(From, [{where, Fn}, {order, {Idx, Order}}, {limit, {Offset, Length}}]) ->
    {atomic, Recs} = mnesia:transaction(fun()->
        QC = qlc:cursor(qlc:q(
            [R || R <- qlc:keysort(Idx, mnesia:table(From), [{order, Order}]), apply(Fn, [R])])),
        qlc:next_answers(QC, Offset - 1),
        Ret = qlc:eval(qlc:next_answers(QC, Length)),
        qlc:delete_cursor(QC),
        Ret
    end),
    Recs;
select(RecordName, Key) ->
    many(fun() -> mnesia:read({RecordName, Key}) end).

get(RecordName, Key) -> just_one(fun() -> mnesia:read(RecordName, Key) end).

get_word(Word) ->
    case mnesia:transaction(fun() -> mnesia:match_object(#ut_word{english = Word, lang = '_', word = Word}) end) of
        {atomic, []} -> {error, not_found};
        {atomic, [R]} -> {ok, R};
        {atomic, [A|_]} -> A;
        _ -> {error, not_found}
    end.

get_translation({Lang, Word}) -> get(ut_translation, {Lang, Word}).
count(RecordName) -> mnesia:table_info(RecordName, size).

all(RecordName) ->
    flatten(fun() ->
                    Lists = mnesia:all_keys(RecordName),
                    [ mnesia:read({RecordName, G}) || G <- Lists ]
            end).

next_id(RecordName) ->
    next_id(RecordName, 1).

next_id(RecordName, Incr) ->
    [RecordStr] = io_lib:format("~p",[RecordName]),
    mnesia:dirty_update_counter({id_seq, RecordStr}, Incr).

unused_invites() ->
    {atomic, Result} =
        mnesia:transaction(
          fun() ->
                  length(mnesia:match_object(#invite_code{created_user=undefined, _='_'}))
          end),
    Result.

user_by_verification_code(Code) -> just_one(fun() -> mnesia:match_object(#user{verification_code = Code, _='_'}) end).
user_by_facebook_id(FBId) -> just_one(fun() -> mnesia:index_read(user, FBId, facebook_id) end).
user_by_email(Email) -> just_one(fun() -> mnesia:index_read(user, Email, email) end).
user_by_username(Name) -> just_one(fun() -> mnesia:match_object(#user{username = Name, _='_'}) end).
invite_code_by_issuer(User) -> just_one(fun() -> mnesia:match_object(#invite_code{issuer = User, _='_'}) end).
invite_code_by_user(User) -> just_one(fun() -> mnesia:match_object(#invite_code{created_user = User, _='_'}) end).

add_to_group(UId, GId, Type) ->
    store:put([#group_member{who = UId,
                                 group = GId,
                                 id = {UId, GId},
                                 type = Type},
                   #group_member_rev{group = GId,
                                     who = UId,
                                     type = Type}]).

remove_from_group(UId, GId) ->
    throw({error, needs_fix}),
    Type = zzzz,
    store:delete([#group_member{who = UId,
                                    group = GId,
                                    id = {UId, GId},
                                    type = Type},
                      #group_member_rev{group = GId,
                                        who = UId,
                                        type = Type}]).

list_membership(#user{username = UId}) -> list_membership(UId);
list_membership(UId) when is_list(UId) -> store:select(group_member, UId).
list_membership_count(#user{username = UId}) -> list_membership_count(UId);
list_membership_count(UId) when is_list(UId) -> [{G, length(list_group_users(element(3, G)))} || G <-store:select(group_member, UId)].
list_group_users(GId) -> store:select(group_member_rev, GId).
membership(UserId, GroupId) -> just_one(fun() -> mnesia:match_object(#group_member{id = {UserId, GroupId}, _='_'}) end).
get_save_tables(UId) -> store:select(save_game_table,UId).
ave_game_table_by_id(Id) -> just_one(fun() -> mnesia:match_object(#save_game_table{id = Id, _='_'}) end).

just_one(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, []} -> {error, not_found};
        {atomic, [R]} -> {ok, R};
        {atomic, [_|_]} -> {error, duplicated};
        _ -> {error, not_found}
    end.

flatten(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, R} -> lists:flatten(R);
        _ -> []
    end.

many(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, R} -> R;
        _ -> []
    end.

void(Fun) ->
    case mnesia:transaction(Fun) of
        {atomic, ok} -> ok;
        {aborted, Error} -> {error, Error}
    end.

create_table(Record, RecordInfo,  Opts0) ->
    Attr = [{attributes, RecordInfo}],
    Opts = transform_opts(Opts0),
    AllOpts = lists:concat([Opts, Attr]),
    case mnesia:create_table(Record, lists:flatten(AllOpts)) of
        {atomic,ok}                          -> ok;
        {aborted, {already_exists, Record}}  -> ok;
        {aborted, Err}                       -> {error, Err}
    end.

add_table_index(Record, Field) ->
    case mnesia:add_table_index(Record, Field) of
        {atomic, ok}                        -> ok;
        {aborted,{already_exists,Record,_}} -> ok;
        {aborted, Err}                       -> {error, Err}
    end.

transform_opts(Opts) -> transform_opts(Opts, []).
transform_opts([], Acc) -> lists:reverse(Acc);
transform_opts([{storage, Value} | Rest], Acc0) ->
    NewOpts = storage_to_mnesia_type(Value),
    Acc = [NewOpts | Acc0],
    transform_opts(Rest, Acc);
transform_opts([Other | Rest], Acc0) ->
    Acc = [Other | Acc0],
    transform_opts(Rest, Acc).

storage_to_mnesia_type(permanent) -> {disc_copies, [node()]};
storage_to_mnesia_type(temporary) -> {ram_copies, [node()]};
storage_to_mnesia_type(ondisk) -> {disc_only_copies, [node()]}.

exec(Q) -> F = fun() -> qlc:e(Q) end, {atomic, Val} = mnesia:transaction(F), Val.

subscribe_user(MeId, FrId) -> store:put([#subscription{who = MeId, whom = FrId},
                              #subscription_rev{whom = FrId, who = MeId}]).

remove_subscription(MeId, FrId) -> store:delete([#subscription{who = MeId, whom = FrId},
                                   #subscription_rev{whom = FrId, who = MeId}]).

list_subscriptions(#user{username = UId}) -> list_subscriptions(UId);
list_subscriptions(UId) when is_list(UId) -> select(subscription,UId).
list_subscription_me(UId) -> select(subscription_rev, UId).

is_user_subscribed(Who, Whom) ->
    case select(subscription,fun(#subscription{who=W1,whom=W2}) when W1=:=Who,W2=:=Whom->true;(_)->false end) of
        [] -> false
        ;_ -> true
    end.

% feeds

feed_add_direct_message(FId, User, Desc, Medias) ->
    EId = store:next_id(entry, 1),
    Entry  = #entry{id = {EId, FId},
                    entry_id = EId,
                    feed_id = FId,
                    from = User,
                    media = Medias,
                    created_time = now(),
                    description = Desc,
                    type = {user, direct}},

    ModEntry = feedformat:format(Entry),
    case store:put(ModEntry) of
        ok ->
            {ok, ModEntry}
        % ;Error -> {error, Error} %% put always return true
    end.

feed_add_entry(FId, User, Desc, Medias) ->
    EId = store:next_id(entry, 1),
    Entry  = #entry{id = {EId, FId},
                    entry_id = EId,
                    feed_id = FId,
                    from = User,
                    media = Medias,
                    created_time = now(),
                    description = Desc},

    ModEntry = feedformat:format(Entry),
    case store:put(ModEntry) of
        ok ->
            {ok, ModEntry}
        % ;Error -> {error, Error} %% put always return true
    end.

entry_by_id(EntryId) -> just_one(fun() -> mnesia:match_object(#entry{entry_id = EntryId, _='_'}) end).
comment_by_id(CommentId) -> just_one(fun() -> mnesia:match_object(#comment{comment_id = CommentId, _='_'}) end).
comments_by_entry(EntryId) -> lists:reverse(many(fun() -> mnesia:match_object(#comment{entry_id = EntryId, _='_'}) end)).
feed_entries(FeedId) -> feed_entries(FeedId, '_').
feed_entries(FeedId, UserId) -> lists:reverse(many(fun() -> mnesia:match_object(#entry{feed_id = FeedId, from = UserId, _='_'}) end)).
feed_entries(FId, Page, PageAmount) -> entries_in_feed(FId, Page, PageAmount).
feed_entries(FId, Page, PageAmount, CurrentUser, CurrentFId) -> entries_in_feed(FId, Page, PageAmount, CurrentUser, CurrentFId).

entries_in_feed(FId, 1, PageAmount) ->
    F=fun(X) when element(4,X)=:=FId-> true;(_)->false end,
    store:select(entry,[{where, F},{order, {1, descending}},{limit, {1,PageAmount}}]);
entries_in_feed(FId, Page, PageAmount) when is_integer(Page), Page>1->
    Offset=(Page-1)*PageAmount,
    F=fun(X) when element(4,X)=:=FId-> true;(_)->false end,
    store:select(entry,[{where, F},{order, {1, descending}},{limit, {Offset,PageAmount}}]).
entries_in_feed(FId, 1, PageAmount, CurrentUser, CurrentFId) ->
    F=fun(X) when element(4,X)=:=FId andalso (element(9,X)=:={user,normal} orelse element(1,element(9,X))=:=system)-> true;
         (X) when element(5,X)=:=CurrentUser,element(9,X)=:={user,direct}-> true;
         (X) when element(4,X)=:=CurrentFId,element(9,X)=:={user,direct} -> true;
         (_)->false end,
    store:select(entry,[{where, F},{order, {1, descending}},{limit, {1,PageAmount}}]);
entries_in_feed(FId, Page, PageAmount, CurrentUser, CurrentFId) when is_integer(Page), Page>1->
    Offset=(Page-1)*PageAmount,
    F=fun(X) when element(4,X)=:=FId andalso (element(9,X)=:={user,normal} orelse element(1,element(9,X))=:=system)-> true;
         (X) when element(5,X)=:=CurrentUser,element(9,X)=:={user,direct}-> true;
         (X) when element(4,X)=:=CurrentFId,element(9,X)=:={user,direct} -> true;
         (_)->false end,
    store:select(entry,[{where, F},{order, {1, descending}},{limit, {Offset,PageAmount}}]).

feed_direct_messages(_FId, Page, PageAmount, CurrentUser, CurrentFId) when is_integer(Page), Page>0->
    Offset= case (Page-1)*PageAmount of
        0 -> 1
        ;M-> M
    end,
    F=fun(X) when element(5,X)=:=CurrentUser,element(9,X)=:={user,direct}-> true;
         (X) when element(4,X)=:=CurrentFId,element(9,X)=:={user,direct} -> true;
         (_)->false end,
    store:select(entry,[{where, F},{order, {1, descending}},{limit, {Offset,PageAmount}}]).

acl_add_entry(_A,_B,_C) -> ok.
