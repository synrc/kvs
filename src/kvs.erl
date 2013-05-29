-module(kvs).
-author('Maxim Sokhatsky <maxim@synrc.com>').
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/translations.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/acls.hrl").
-include_lib("kvs/include/meetings.hrl").
-include_lib("kvs/include/invites.hrl").
-include_lib("kvs/include/config.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/log.hrl").
-include_lib("kvs/include/membership_packages.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

-define(DBA, store_riak).

start() -> DBA = ?DBA, DBA:start().
dir() -> DBA = ?DBA, DBA:dir().
purchases(UserId) -> DBA = ?DBA, DBA:purchases(UserId).
transactions(UserId) -> DBA = ?DBA, DBA:transactions(UserId).
stop() -> DBA = ?DBA, DBA:stop().
initialize() -> DBA = ?DBA, DBA:initialize().
delete() -> DBA = ?DBA, DBA:delete().
init_indexes() -> DBA = ?DBA, DBA:init_indexes().

init_db() ->
    case kvs:get(user,"alice") of
       {error,_} ->
            DBA = ?DBA,
            DBA:init_db(),
            add_seq_ids(),
            accounts:create_account(system),
            add_sample_users(),
            add_sample_packages(),
            add_translations(),
            case is_production() of
                false ->
                    add_purchases();
                true ->
                    do_nothing
            end;
       {ok,_} -> ignore
    end.

is_production() ->
    case kvs:get(config, "debug/production", false) of
        {ok, true} -> true;
        _ -> false
    end.

add_purchases() ->
    {ok, Pkg1} = membership_packages:get_package(1),
    {ok, Pkg2} = membership_packages:get_package(2),
    {ok, Pkg3} = membership_packages:get_package(3),
    {ok, Pkg4} = membership_packages:get_package(4),
    PList = [{"doxtop", Pkg1},{"maxim", Pkg2},{"maxim",Pkg4}, {"kate", Pkg3} ],
    [ok = add_purchase(U, P) || {U, P} <- PList],
    ok.

add_purchase(UserId, Package) ->
    {ok, MPId} = membership_packages:add_purchase(
                         #membership_purchase{user_id=UserId, membership_package=Package }),
    membership_packages:set_purchase_state(MPId, ?MP_STATE_DONE, undefined).



add_seq_ids() ->
    Init = fun(Key) ->
           case kvs:get(id_seq, Key) of
                {error, _} -> ok = kvs:put(#id_seq{thing = Key, id = 0});
                {ok, _} -> ignore
           end
    end,
    Init("meeting"),
    Init("user_transaction"),
    Init("transaction"),
    Init("membership_purchase"),
    Init("acl"),
    Init("acl_entry"),
    Init("feed"),
    Init("entry"),
    Init("like_entry"),
    Init("likes"),
    Init("one_like"),
    Init("comment"),
    Init("save_table").

add_translations() ->
    lists:foreach(fun({English, Lang, Word}) ->
                          ok = kvs:put(#translation{english = English, lang = "en",  word = English}),
                          ok = kvs:put(#translation{english = English, lang = Lang,  word = Word}),
              ok
    end, ?URL_DICTIONARY).

add_sample_users() ->
    UserList = [
                     #user{username = "maxim", password="kaka15ra",
                           name = "Maxim", surname = "Sokhatsky", feed = feed_create(),
                           type = admin, direct = feed_create(),
                           sex=m,
                           status=ok,
                           team = create_team("tours"),
                           email="maxim.sokhatsky@gmail.com"},
                     #user{username = "doxtop", password="password",
                           feed = feed_create(),
                           name = "Andrii Zadorozhnii",
                           email="doxtop@synrc.com",
                           type=admin,
                           team = create_team("tours"), direct = feed_create(),
                           status=ok,
                           age={1981,9,29},
                           register_date={1345,14071,852889}
                     } 
         ],


    ?INFO("creating groups"),

    GId1  = groups:create_group_directly_to_db("maxim", "kakaranet", "Kakaranet", "Kakaranet'e Hoşgeldiniz", public),
    GId2  = groups:create_group_directly_to_db("maxim", "yeniler", "Yeniler", "So, you must be new here.", public),

    ?INFO("adding users accounts"),
    [ begin
          accounts:create_account(Me#user.username),
          accounts:transaction(Me#user.username, quota, kvs:get_config("accounts/default_quota", 300), #tx_default_assignment{}),
          kvs:put(Me#user{password = utils:sha(Me#user.password),
                                starred = feed_create(),
                                pinned = feed_create()})
      end || Me <- UserList],
    ?INFO("adding users to groups"),
    [ begin
          users:init_mq(Me#user.username, [GId1, GId2]),
          groups:add_to_group_directly_to_db(Me#user.username, GId1, member),
          groups:add_to_group_directly_to_db(Me#user.username, GId2, member)
      end || Me <- UserList ],
    acls:define_access({user, "maxim"},    {feature, admin}, allow),
    acls:define_access({user_type, admin}, {feature, admin}, allow),
    ?INFO("making all users each other friends"),
    [[case Me == Her of
        true -> ok;
        false -> userss:subscr_user(Me#user.username, Her#user.username)
    end || Her <- UserList] || Me <- UserList].

add_sample_packages() -> membership_packages:add_sample_data().
version() -> ?INFO("version: ~p", [1]).

% blocking

block_user(Who, Whom) -> DBA=?DBA, DBA:block_user(Who, Whom).
list_blocks(Who) -> DBA=?DBA, DBA:list_blocks(Who).
unblock_user(Who, Whom) -> DBA=?DBA, DBA:unblock_user(Who, Whom).
list_blocked_me(Me) -> DBA=?DBA, DBA:list_blocked_me(Me).
is_user_blocked(Who, Whom) -> DBA=?DBA, DBA:is_user_blocked(Who, Whom).

% configs

add_configs() ->
    %% smtp
    kvs:put(#config{key="smtp/user",     value="noreply@synrc.com"}),
    kvs:put(#config{key="smtp/password", value="maxim@synrc.com"}),
    kvs:put(#config{key="smtp/host",     value="mail.synrc.com"}),
    kvs:put(#config{key="smtp/port",     value=465}),
    kvs:put(#config{key="smtp/with_ssl", value=true}),
    kvs:put(#config{key="accounts/default_quota", value=2000}),
    kvs:put(#config{key="accounts/quota_limit/soft",  value=-30}),
    kvs:put(#config{key="accounts/quota_limit/hard",  value=-100}),
    kvs:put(#config{key="purchase/notifications/email",  value=["maxim@synrc.com"]}),
    kvs:put(#config{key="delivery/notifications/email",  value=["maxim@synrc.com"]}).

put(Record) ->
    DBA=?DBA,
    DBA:put(Record).

put_if_none_match(Record) ->
    DBA=?DBA,
    DBA:put_if_none_match(Record).

update(Record, Meta) ->
    DBA=?DBA,
    DBA:update(Record, Meta).

get(RecordName, Key) ->
    DBA=?DBA,
    case C = DBA:get(RecordName, Key) of
        {ok,_R} -> C;
        A -> A end.

get_for_update(RecordName, Key) ->
    DBA=?DBA,
    DBA:get_for_update(RecordName, Key).

get(RecordName, Key, Default) ->
    DBA=?DBA,
    case DBA:get(RecordName, Key) of
        {ok,{RecordName,Key,Value}} ->
            ?INFO("db:get config value ~p,", [{RecordName, Key, Value}]),
            {ok,Value};
        {error, _B} ->
            ?INFO("db:get new config value ~p,", [{RecordName, Key, Default}]),
            DBA:put({RecordName,Key,Default}),
            {ok,Default} end.

get_config(Key, Default) -> {ok, Value} = get(config, Key, Default), Value.
get_word(Word) -> get(ut_word,Word).
get_translation({Lang,Word}) -> DBA=?DBA, DBA:get_translation({Lang,Word}).

% delete

delete(Keys) -> DBA=?DBA, DBA:delete(Keys).
delete(Tab, Key) -> ?INFO("db:delete ~p:~p",[Tab, Key]), DBA=?DBA,DBA:delete(Tab, Key).
delete_by_index(Tab, IndexId, IndexVal) -> DBA=?DBA,DBA:delete_by_index(Tab, IndexId, IndexVal).
% select

multi_select(RecordName, Keys) -> DBA=?DBA,DBA:multi_select(RecordName, Keys).
select(From, PredicateFunction) -> ?INFO("db:select ~p, ~p",[From,PredicateFunction]), DBA=?DBA, DBA:select(From, PredicateFunction).
count(RecordName) -> DBA=?DBA,DBA:count(RecordName).
all(RecordName) -> DBA=?DBA,DBA:all(RecordName).
all_by_index(RecordName, Index, IndexValue) -> DBA=?DBA,DBA:all_by_index(RecordName, Index, IndexValue).

% id generator

next_id(RecordName) -> DBA=?DBA,DBA:next_id(RecordName).
next_id(RecordName, Incr) -> DBA=?DBA,DBA:next_id(RecordName, Incr).
next_id(RecordName, Default, Incr) -> DBA=?DBA,DBA:next_id(RecordName, Default, Incr).

% browser counter

delete_browser_counter_older_than(MinTS) -> DBA=?DBA,DBA:delete_browser_counter_older_than(MinTS).
browser_counter_by_game(Game) -> DBA=?DBA,DBA:browser_counter_by_game(Game).

% invites

unused_invites() -> DBA=?DBA,DBA:unused_invites().
user_by_verification_code(Code) -> DBA=?DBA,DBA:user_by_verification_code(Code).
user_by_facebook_id(FBId) -> DBA=?DBA,DBA:user_by_facebook_id(FBId).
user_by_email(Email) -> DBA=?DBA,DBA:user_by_email(Email).
user_by_username(Name) -> DBA=?DBA,DBA:user_by_username(Name).
add_invite_to_issuer(User, O) -> DBA=?DBA,DBA:add_invite_to_issuer(User, O).
invite_code_by_issuer(User) -> DBA=?DBA,DBA:invite_code_by_issuer(User).
invite_code_by_user(User) -> DBA=?DBA,DBA:invite_code_by_user(User).

% game info

get_save_tables(Id) -> DBA=?DBA,DBA:get_save_tables(Id).
save_game_table_by_id(Id) -> DBA=?DBA,DBA:save_game_table_by_id(Id).

% feeds

feed_add_direct_message(FId, User, To, EntryId, Desc, Medias) -> DBA=?DBA,DBA:feed_add_direct_message(FId, User, To, EntryId, Desc, Medias).
feed_add_entry(FId, User, EntryId, Desc, Medias) -> DBA=?DBA,DBA:feed_add_entry(FId, User, EntryId, Desc, Medias).
feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy) -> DBA=?DBA,DBA:feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy).
acl_add_entry(AclId, Accessor, Action) -> DBA=?DBA,DBA:acl_add_entry(AclId, Accessor, Action).
acl_entries(AclId) -> DBA=?DBA,DBA:acl_entries(AclId).
entry_by_id(EntryId) -> DBA=?DBA,DBA:entry_by_id(EntryId).
comment_by_id(CommentId) -> DBA=?DBA,DBA:comment_by_id(CommentId).
comments_by_entry({_EId, _FId} = EntryId) -> DBA=?DBA,DBA:comments_by_entry(EntryId).
entries_in_feed(FeedId) -> DBA=?DBA,DBA:entries_in_feed(FeedId, undefined, all).
entries_in_feed(FeedId, Count) -> DBA=?DBA,DBA:entries_in_feed(FeedId, undefined, Count).
entries_in_feed(FeedId, StartFrom, Count) -> DBA=?DBA, DBA:entries_in_feed(FeedId, StartFrom, Count).
add_comment(FId, User, EntryId, ParentComment, CommentId, Content, Medias) -> DBA=?DBA, DBA:feed_add_comment(FId, User, EntryId, ParentComment, CommentId, Content, Medias).
feed_direct_messages(FId, StartFrom, Count) ->  DBA=?DBA, DBA:entries_in_feed(FId, StartFrom, Count).

% tournaments

tournament_waiting_queue(TID) -> DBA=?DBA, DBA:tournament_waiting_queue(TID).
join_tournament(UID,TID) -> DBA=?DBA, DBA:join_tournament(UID,TID).
leave_tournament(UID,TID) -> DBA=?DBA, DBA:leave_tournament(UID,TID).
tournament_pop_waiting_player(TID) -> DBA=?DBA, DBA:tournament_pop_waiting_player(TID).
user_tournaments(UID) -> DBA=?DBA, DBA:user_tournaments(UID).

add_transaction_to_user(User, Tx) -> DBA=?DBA, DBA:add_transaction_to_user(User, Tx).
get_purchases_by_user(User, Count, States) -> DBA=?DBA, DBA:get_purchases_by_user(User, Count, States).
get_purchases_by_user(User, StartFromPurchase, Count, States) -> DBA=?DBA, DBA:get_purchases_by_user(User, StartFromPurchase, Count, States).

make_admin(User) ->
    {ok,U} = kvs:get(user, User),
    kvs:put(U#user{type = admin}),
    acls:define_access({user, U#user.username}, {feature, admin}, allow),
    acls:define_access({user_type, admin}, {feature, admin}, allow),
    ok.

make_rich(User) -> 
    Q = kvs:get_config("accounts/default_quota",  300),
    accounts:transaction(User, quota, Q * 100, #tx_default_assignment{}),
    accounts:transaction(User, internal, Q, #tx_default_assignment{}),
    accounts:transaction(User, currency, Q * 2, #tx_default_assignment{}).

feed_create() ->
    FId = kvs:next_id("feed", 1),
    ok = kvs:put(#feed{id = FId} ),
    FId.

create_team(Name) ->
    TID = kvs:next_id("team",1),
    ok = kvs:put(Team = #team{id=TID,name=Name}),
    TID.

list_to_term(String) ->
    {ok, T, _} = erl_scan:string(String++"."),
    case erl_parse:parse_term(T) of
        {ok, Term} ->
            Term;
        {error, Error} ->
            Error
    end.

save_db(Path) ->
    Data = lists:append([all(B) || B <- list_buckets()]),
    kvs:save(Path, Data).

load_db(Path) ->
    add_seq_ids(),
    AllEntries = kvs:load(Path),
    [{_,_,{_,Handler}}] = ets:lookup(config, "riak_client"),
    [case is_tuple(E) of
        false -> skip;
        true ->  put(E) 
    end || E <- AllEntries].

make_paid_fake(UId) ->
    put({user_purchase, UId, "fake_purchase"}).

save(Key, Value) ->
    Dir = ling:trim_from_last(Key, "/"),
    filelib:ensure_dir(Dir),
    file:write_file(Key, term_to_binary(Value)).

load(Key) ->
    {ok, Bin} = file:read_file(Key),
    binary_to_term(Bin).
