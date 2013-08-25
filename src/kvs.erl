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
-include_lib("kvs/include/membership.hrl").
-include_lib("kvs/include/payments.hrl").
-include_lib("kvs/include/products.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("kvs/include/feed_state.hrl").
-compile(export_all).

start() -> DBA = ?DBA, DBA:start().
dir() -> DBA = ?DBA, DBA:dir().
stop() -> DBA = ?DBA, DBA:stop().
initialize() -> DBA = ?DBA, DBA:initialize().
delete() -> DBA = ?DBA, DBA:delete().
init_indexes() -> DBA = ?DBA, DBA:init_indexes().

add(Record) when is_tuple(Record) ->
    Id = element(#iterator.id, Record),

    case kvs:get(element(1,Record), Id) of {ok, _} -> error_logger:info_msg("Exist: ~p", [Id]),{error, exist};
    {error, not_found} ->
        Type = element(1, Record),
        CName = element(#iterator.container, Record),
        Cid = case element(#iterator.feed_id, Record) of undefined -> ?FEED(Type); Fid -> Fid end,
        error_logger:info_msg("check container ~p ~p", [CName, Cid]),
        Container = case kvs:get(CName, Cid) of {ok,C} -> error_logger:info_msg("ok"),C;
        {error, not_found} when Cid /= undefined ->
            NC =  setelement(#container.id, erlang:list_to_tuple([CName|proplists:get_value(CName, ?CONTAINERS)]), Cid),
            NC1 = setelement(#container.entries_count, NC, 0),
            error_logger:info_msg("Create top: ~p", [NC1]),
            kvs:put(NC1),NC1;
        _ -> error end,

        if Container == error -> {error, no_container}; true ->
            error_logger:info_msg("container ~p", [Container]),
            Next = undefined,
            Prev = case element(#container.top, Container) of undefined -> undefined;
              Tid -> case kvs:get(Type, Tid) of {error, not_found} -> undefined;
                {ok, Top} -> NewTop = setelement(#iterator.next, Top, Id), kvs:put(NewTop), element(#iterator.id, NewTop) end end,
            error_logger:info_msg("next ~p | prev ~p",[Next,Prev]),

            C1 = setelement(#container.top, Container, Id),
            C2 = setelement(#container.entries_count, C1, element(#container.entries_count, Container)+1),
            error_logger:info_msg("updated container: ~p", [C2]),
            kvs:put(C2),

            R  = setelement(#iterator.feeds, Record, [{F1, kvs_feed:create()} || F1 <- element(#iterator.feeds, Record)]),
            R1 = setelement(#iterator.next,  R,  Next),
            R2 = setelement(#iterator.prev,  R1, Prev),
            R3 = setelement(#iterator.feed_id, R2, element(#container.id, Container)),
            kvs:put(R3),
            error_logger:info_msg("PUT: ~p", [R3]),
            {ok, R3} end end.

remove(RecordName, RecordId) ->
    error_logger:info_msg("Remove ~p ~p", [RecordName, RecordId]),
    case kvs:get(RecordName, RecordId) of {error, not_found} -> error_logger:info_msg("not found");
    {ok, E} ->
        Id = element(#iterator.id, E),
        CName = element(#iterator.container, E),
        Cid = element(#iterator.feed_id, E),

        {ok, Container} = kvs:get(CName, Cid),
        Top = element(#container.top, Container),

        Next = element(#iterator.next, E),
        Prev = element(#iterator.prev, E),
        case kvs:get(RecordName, Next) of {ok, NE} -> NewNext = setelement(#iterator.prev, NE, Prev), kvs:put(NewNext); _ -> ok end,
        case kvs:get(RecordName, Prev) of {ok, PE} -> NewPrev = setelement(#iterator.next, PE, Next), kvs:put(NewPrev); _ -> ok end,

        C1 = case Top of Id -> setelement(#container.top, Container, Prev); _ -> Container end,
        C2 = setelement(#container.entries_count, C1, element(#container.entries_count, Container)-1),
        kvs:put(C2),
        error_logger:info_msg("Remove record ~p id: ~p", [RecordName, Id]),
        kvs:delete(RecordName, Id) end.

%purge_feed(FeedId) ->
%    {ok,Feed} = kvs:get(feed,FeedId),
%    Removal = entry_traversal(Feed#feed.top, -1),
%    [kvs:delete(entry,Id)||#entry{id=Id}<-Removal],
%    kvs:put(Feed#feed{top=undefined}).
%purge_unverified_feeds() ->
%    [ [purge_feed(Fid)|| {_, Fid} <- Feeds ] || #user{feeds=Feeds, email=E} <- kvs:all(user), E==undefined].

traversal( _,undefined,_) -> [];
traversal(_,_,0) -> [];
traversal(RecordType, Start, Count)->
    case kvs:get(RecordType, Start) of {error,_} -> [];
    {ok, R} ->  Prev = element(#iterator.prev, R),
                Count1 = case Count of C when is_integer(C) -> C - 1; _-> Count end,
                [R | traversal(RecordType, Prev, Count1)] end.

entries(Container, RecordType) -> entries(Container, RecordType, undefined).
entries({ok, Container}, RecordType, Count) -> entries(Container, RecordType, Count);

entries({_, FeedId}, undefined, Count) -> entries(kvs:get(feed, FeedId), entry, Count);
entries({_, FeedId}, StartFrom, Count) ->
    case kvs:get(entry,{StartFrom, FeedId}) of {error,not_found}->[];
    {ok, E} -> traversal(entry, element(#iterator.prev, E), Count) end;

entries(Container, RecordType, Count) -> traversal(RecordType, element(#container.top, Container), Count).
entries(CName, Cid, RecordType, Count) -> case kvs:get(CName, Cid) of {ok, C}-> entries(C, RecordType, Count); {error, _} -> [] end.

init_db() ->
    case kvs:get(user,"joe") of
        {error,_} ->
            add_seq_ids(),
            kvs_account:create_account(system),
            %add_sample_users(),
%            add_sample_packages(),
%            add_sample_payments(),
            add_translations();
        {ok,_} -> ignore end.

%add_sample_packages() -> kvs_membership:add_sample_data().

%add_sample_payments() ->
%    {ok, Pkg1} = kvs:get(membership,1),
%    {ok, Pkg2} = kvs:get(membership,2),
%    {ok, Pkg3} = kvs:get(membership,3),
%    {ok, Pkg4} = kvs:get(membership,4),
%    PList = [{"doxtop", Pkg1},{"maxim", Pkg2},{"maxim",Pkg4}, {"kate", Pkg3} ],
%    [ok = add_payment(U, P) || {U, P} <- PList],
%    ok.

%add_payment(UserId, Package) ->
%    {ok, MPId} = kvs_payment:add_payment(#payment{user_id=UserId, membership=Package}),
%    kvs_payment:set_payment_state(MPId, ?MP_STATE_DONE, undefined).

add_seq_ids() ->
    Init = fun(Key) ->
           case kvs:get(id_seq, Key) of
                {error, _} -> ok = kvs:put(#id_seq{thing = Key, id = 0});
                {ok, _} -> ignore
           end
    end,
    Init("meeting"),
    Init("user_transaction"),
    Init("user_product"),
    Init("user_payment"),
    Init("user_status"),
    Init("transaction"),
    Init("membership"),
    Init("payment"),
    Init("acl"),
    Init("acl_entry"),
    Init("feed"),
    Init("entry"),
    Init("like_entry"),
    Init("likes"),
    Init("one_like"),
    Init("comment").

add_translations() ->
    lists:foreach(fun({English, Lang, Word}) ->
        kvs:put(#translation{english = English, lang = "en",  word = English}),
        kvs:put(#translation{english = English, lang = Lang,  word = Word}) end, ?URL_DICTIONARY).

add_sample_users() ->
  Groups = [],
  UserList = [],

  kvs:put(Groups),

  {ok, Quota} = kvs:get(config,"accounts/default_quota", 300),

  [ begin
        [ kvs_group:join(Me#user.username,G#group.id) || G <- Groups ],
          kvs_account:create_account(Me#user.username),
          kvs_account:transaction(Me#user.username, quota, Quota, #tx_default_assignment{}),
          kvs:put(Me#user{password = kvs:sha(Me#user.password)})
    end || Me <- UserList ],

  %kvs_acl:define_access({user, "maxim"},    {feature, admin}, allow),
  %kvs_acl:define_access({user_type, admin}, {feature, admin}, allow),

  [ kvs_user:subscribe(Me#user.username, Her#user.username) || Her <- UserList, Me <- UserList, Her /= Me ],
  [ kvs_user:init_mq(U) || U <- UserList ],

  ok.

version() -> DBA=?DBA, DBA:version().

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
    DBA:get(RecordName, Key).

get_for_update(RecordName, Key) ->
    DBA=?DBA,
    DBA:get_for_update(RecordName, Key).

get(RecordName, Key, Default) ->
    DBA=?DBA,
    case DBA:get(RecordName, Key) of
        {ok,{RecordName,Key,Value}} ->
            error_logger:info_msg("db:get config value ~p,", [{RecordName, Key, Value}]),
            {ok,Value};
        {error, _B} ->
            error_logger:info_msg("db:get new config value ~p,", [{RecordName, Key, Default}]),
            DBA:put({RecordName,Key,Default}),
            {ok,Default} end.

delete(Keys) -> DBA=?DBA, DBA:delete(Keys).
delete(Tab, Key) -> error_logger:info_msg("db:delete ~p:~p",[Tab, Key]), DBA=?DBA,DBA:delete(Tab, Key).
delete_by_index(Tab, IndexId, IndexVal) -> DBA=?DBA,DBA:delete_by_index(Tab, IndexId, IndexVal).
multi_select(RecordName, Keys) -> DBA=?DBA,DBA:multi_select(RecordName, Keys).
select(From, PredicateFunction) -> error_logger:info_msg("db:select ~p, ~p",[From,PredicateFunction]), DBA=?DBA, DBA:select(From, PredicateFunction).
count(RecordName) -> DBA=?DBA,DBA:count(RecordName).
all(RecordName) -> DBA=?DBA,DBA:all(RecordName).
all_by_index(RecordName, Index, IndexValue) -> DBA=?DBA,DBA:all_by_index(RecordName, Index, IndexValue).
next_id(RecordName) -> DBA=?DBA,DBA:next_id(RecordName).
next_id(RecordName, Incr) -> DBA=?DBA,DBA:next_id(RecordName, Incr).
next_id(RecordName, Default, Incr) -> DBA=?DBA,DBA:next_id(RecordName, Default, Incr).

author_comments(Who) -> DBA=?DBA,DBA:author_comments(Who).

make_admin(User) ->
    {ok,U} = kvs:get(user, User),
    kvs_acl:define_access({user, U#user.id}, {feature, admin}, allow),
    ok.

make_rich(User) -> 
    Q = kvs:get_config("accounts/default_quota",  300),
    kvs_account:transaction(User, quota, Q * 100, #tx_default_assignment{}),
    kvs_account:transaction(User, internal, Q, #tx_default_assignment{}),
    kvs_account:transaction(User, currency, Q * 2, #tx_default_assignment{}).

list_to_term(String) ->
    {ok, T, _} = erl_scan:string(String++"."),
    case erl_parse:parse_term(T) of
        {ok, Term} ->
            Term;
        {error, Error} ->
            Error
    end.

save_db(Path) ->
    Data = lists:append([all(B) || B <- [list_to_term(B) || B <- store_riak:dir()] ]),
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
    put(#payment{user_id=UId,info= "fake_purchase"}).

save(Dir, Value) ->
    filelib:ensure_dir(Dir),
    file:write_file(Dir, term_to_binary(Value)).

load(Key) ->
    {ok, Bin} = file:read_file(Key),
    binary_to_term(Bin).

coalesce(undefined, B) -> B;
coalesce(A, _) -> A.


uuid() ->
  R1 = random:uniform(round(math:pow(2, 48))) - 1,
  R2 = random:uniform(round(math:pow(2, 12))) - 1,
  R3 = random:uniform(round(math:pow(2, 32))) - 1,
  R4 = random:uniform(round(math:pow(2, 30))) - 1,
  R5 = erlang:phash({node(), now()}, round(math:pow(2, 32))),

  UUIDBin = <<R1:48, 4:4, R2:12, 2:2, R3:32, R4: 30>>,
  <<TL:32, TM:16, THV:16, CSR:8, CSL:8, N:48>> = UUIDBin,

  lists:flatten(io_lib:format("~8.16.0b-~4.16.0b-~4.16.0b-~2.16.0b~2.16.0b-~12.16.0b-~8.16.0b",
                              [TL, TM, THV, CSR, CSL, N, R5])).
uuname() ->
  lists:flatten(io_lib:format("~8.16.0b",[erlang:phash2({node(), now()}, round(math:pow(2, 32)))])).

sha(Raw) ->
    lists:flatten([io_lib:format("~2.16.0b", [N]) || <<N>> <= crypto:sha(Raw)]).

sha_upper(Raw) ->
    SHA = sha(Raw),
    string:to_upper(SHA).
