-module(kvs_feed).
-compile(export_all).
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feed_state.hrl").
-include_lib("kvs/include/log.hrl").

create() ->
    FId = kvs:next_id("feed", 1),
    ok = kvs:put(#feed{id = FId} ),
    FId.

add_direct_message(FId, User, Desc) -> add_direct_message(FId, User, utils:uuid_ex(), Desc).
add_direct_message(FId, User, EntryId, Desc) -> add_direct_message(FId, User, undefined, EntryId, Desc, []).
add_direct_message(FId, User, To, EntryId, Desc, Medias) -> kvs:feed_add_direct_message(FId, User, To, EntryId, Desc, Medias).
add_group_entry(FId, User, EntryId, Desc, Medias) -> kvs:feed_add_entry(FId, User, EntryId, Desc, Medias).
add_group_entry(FId, User, To, EntryId, Desc, Medias, Type) -> kvs:feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, "").
add_entry(FId, User, EntryId, Desc) -> add_entry(FId, User, EntryId, Desc, []).
add_entry(FId, User, EntryId, Desc, Medias) -> kvs:feed_add_entry(FId, User, EntryId, Desc, Medias).
add_entry(FId, User, To, EntryId, Desc, Medias, Type) -> kvs:feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, "").
add_shared_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy) -> kvs:feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy).

add_like(Fid, Eid, Uid) ->
    Write_one_like = fun(Next) ->
        Self_id = kvs:next_id("one_like", 1),   
        kvs:put(#one_like{    % add one like
            id = Self_id,
            user_id = Uid,
            entry_id = Eid,
            feed_id = Fid,
            created_time = now(),
            next = Next
        }),
        Self_id
    end,
    % add entry - like
    case kvs:get(entry_likes, Eid) of
        {ok, ELikes} -> 
            kvs:put(ELikes#entry_likes{
                one_like_head = Write_one_like(ELikes#entry_likes.one_like_head), 
                total_count = ELikes#entry_likes.total_count + 1
            });
        {error, notfound} ->
            kvs:put(#entry_likes{
                entry_id = Eid,                
                one_like_head = Write_one_like(undefined),
                total_count = 1
            })
    end,
    % add user - like
    case kvs:get(user_likes, Uid) of
        {ok, ULikes} -> 
            kvs:put(ULikes#user_likes{
                one_like_head = Write_one_like(ULikes#user_likes.one_like_head),
                total_count = ULikes#user_likes.total_count + 1
            });
        {error, notfound} ->
            kvs:put(#user_likes{
                user_id = Uid,                
                one_like_head = Write_one_like(undefined),
                total_count = 1
            })
    end.

% statistics

get_entries_count(Uid) ->
    case kvs:get(user_etries_count, Uid) of
        {ok, UEC} -> UEC#user_etries_count.entries;
        {error, notfound} -> 0 end.

get_comments_count(Uid) ->
    case kvs:get(user_etries_count, Uid) of
        {ok, UEC} -> UEC#user_etries_count.comments;
        {error, notfound} -> 0 end.

get_feed(FId) -> kvs:get(feed, FId).
get_entries_in_feed(FId) -> kvs:entries_in_feed(FId).
get_entries_in_feed(FId, Count) -> kvs:entries_in_feed(FId, Count).
get_entries_in_feed(FId, StartFrom, Count) -> kvs:entries_in_feed(FId, StartFrom, Count).
get_direct_messages(FId, Count) -> kvs:entries_in_feed(FId, undefined, Count).
get_direct_messages(FId, StartFrom, Count) -> kvs:entries_in_feed(FId, StartFrom, Count).
get_entries_in_feed(FId, StartFrom, Count, FromUserId)-> Entries = kvs:entries_in_feed(FId, StartFrom, Count),
    [E || #entry{from = From} = E <- Entries, From == FromUserId].

create_message(Table) ->
    EId = kvs:next_id("entry", 1),
    #entry{id = {EId, system_info},
        entry_id = EId,
        from = system,
        type = {system, new_table},
        created_time = now(),
        description = Table}.


remove_entry(FeedId, EId) ->
    {ok, #feed{top = TopId} = Feed} = get_feed(FeedId),

    case kvs:get(entry, {EId, FeedId}) of
        {ok, #entry{prev = Prev, next = Next}}->
            ?INFO("P: ~p, N: ~p", [Prev, Next]),
            case kvs:get(entry, Next) of
                {ok, NE} -> kvs:put(NE#entry{prev = Prev});
                _ -> ok
            end,
            case kvs:get(entry, Prev) of
                {ok, PE} -> kvs:put(PE#entry{next = Next});
                _ -> ok
            end,

            case TopId of
                {EId, FeedId} -> kvs:put(Feed#feed{top = Prev});
                _ -> ok
            end;
        {error, notfound} -> ?INFO("Not found"), ok
    end,
    kvs:delete(entry, {EId, FeedId}).

edit_entry(FeedId, EId, NewDescription) ->
    case kvs:entry_by_id({EId, FeedId}) of
        {ok, OldEntry} ->
            NewEntryRaw =  OldEntry#entry{description = NewDescription,
                                          raw_description = NewDescription},
            NewEntry = feedformat:format(NewEntryRaw),
            kvs:put(NewEntry);
        {error, notfound}->
            {error, notfound}
    end.

remove_entry_comments(FId, EId) ->
    AllComments = kvs:comments_by_entry(FId, EId),
    [begin
          kvs:delete(comment, ID)
     end || #comment{id = ID, media = M} <- AllComments].

entry_add_comment(FId, User, EntryId, ParentComment, CommentId, Content, Medias) ->
     case kvs:entry_by_id({EntryId, FId}) of
         {ok, _E} ->
             kvs:add_comment(FId, User, EntryId, ParentComment, CommentId, Content, Medias);
         _ ->
             ok
     end.

get_one_like_list(undefined) -> [];
get_one_like_list(Id) -> {ok, OneLike} = kvs:get(one_like, Id),
    [OneLike] ++ get_one_like_list(OneLike#one_like.next).

get_entries_likes(Entry_id) ->
    case kvs:get(entry_likes, Entry_id) of
        {ok, Likes} -> get_one_like_list(Likes#entry_likes.one_like_head);
        {error, notfound} -> []
    end.

get_entries_likes_count(Entry_id) ->
    case kvs:get(entry_likes, Entry_id) of
        {ok, Likes} ->
            Likes#entry_likes.total_count;
        {error, notfound} -> 0
    end.

get_user_likes_count(UserId) ->
    case kvs:get(user_likes, UserId) of
        {ok, Likes} -> Likes#user_likes.total_count;
        {error, notfound} -> 0
    end.

get_user_likes(UserId) ->
    case kvs:get(user_likes, UserId) of
        {ok, Likes} -> get_one_like_list(Likes#user_likes.one_like_head);
        {error, notfound} -> []
    end.

get_one_like_list(undefined, _) -> [];
get_one_like_list(_, 0) -> [];
get_one_like_list(Id, N) -> {ok, OneLike} = kvs:get(one_like, Id),
    [OneLike] ++ get_one_like_list(OneLike#one_like.next, N-1).

get_user_likes(UserId, {Page, PageAmount}) ->
    case kvs:get(user_likes, UserId) of
        {ok, Likes} -> lists:nthtail((Page-1)*PageAmount, get_one_like_list(Likes#user_likes.one_like_head, PageAmount*Page));
        {error, notfound} -> []
    end.

% we have same in user? Why?
is_subscribed_user(UserUidWho, UserUidWhom) -> kvs_users:is_user_subscr(UserUidWho, UserUidWhom).
user_subscription_count(UserUid) -> length(kvs_users:list_subscr(UserUid)).
user_friends_count(UserUid) -> length(kvs_users:list_subscr_me(UserUid)).

get_comments_entries(UserUid, _, _Page, _PageAmount) ->
    Pids = [Eid || #comment{entry_id=Eid} <- kvs:select(comment,
        fun(#comment{author_id=Who}) when Who=:=UserUid ->true;(_)->false end)],
    %?PRINT({"GCE pids length: ", length(Pids)}),
    lists:flatten([kvs:select(entry,[{where, fun(#entry{entry_id=ID})-> ID=:=Pid end},
        {order, {1, descending}},{limit, {1,1}}]) || Pid <- Pids]).

get_my_discussions(_FId, Page, PageAmount, UserUid) ->
    _Offset= case (Page-1)*PageAmount of
        0 -> 1
        ;M-> M
    end,
    Pids = [Eid || #comment{entry_id=Eid} <- kvs:select(comment,
        fun(#comment{author_id=Who}) when Who=:=UserUid ->true;(_)->false end)],
    lists:flatten([kvs:select(entry,[{where, fun(#entry{entry_id=ID})-> ID=:=Pid end},
        {order, {1, descending}},{limit, {1,1}}]) || Pid <- Pids]).

test_likes() ->
    add_like(1, "17a10803-f064-4718-ae46-4a6d3c88415c-0796e2bb", "derp"),
    add_like(1, "17a10803-f064-4718-ae46-4a6d3c88415c-0796e2bb", "derpina"),
    add_like(1, "17a10803-f064-4718-ae46-4a6d3c88415c-0796e2bb", "derpington"),
    add_like(1, "17a10803-f064-4718-ae46-4a6d3c88415c-0796e2bc", "derp"),
    add_like(1, "17a10803-f064-4718-ae46-4a6d3c88415c-0796e2bc", "lederpeaux"),
    add_like(2, "17a10803-f064-4718-ae46-4a6d3c88415c-0796e2bd", "derp"),
    add_like(2, "17a10803-f064-4718-ae46-4a6d3c88415c-0796e2be", "derp"),
    [
        get_entries_likes("17a10803-f064-4718-ae46-4a6d3c88415c-0796e2bb"),
        get_entries_likes_count("17a10803-f064-4718-ae46-4a6d3c88415c-0796e2bb") == 3,
        get_user_likes("derp"),
        get_user_likes("derp", {1, 2}),
        get_user_likes_count("derp") == 4
    ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_notice(["feed", "delete", Owner] = Route, Message,
              #state{owner = Owner} = State) ->
    ?INFO("feed(~p): notification received: User=~p, Route=~p, Message=~p",
          [self(), Owner, Route, Message]),
    {stop, normal, State};

handle_notice(["feed", "group", GroupId, "entry", EntryId, "add"] = Route,
              [From|_] = Message,
              #state{owner = Owner, feed = Feed} = State) ->
    ?INFO("feed(~p): group message: Owner=~p, Route=~p, Message=~p",
          [self(), Owner, Route, Message]),
    [From, _Destinations, Desc, Medias] = Message,
    feed:add_group_entry(Feed, From, [{GroupId, group}], EntryId,
                         Desc, Medias, {group, direct}),
    % statistics
    case Owner == GroupId of
        false -> ok;
        true ->
            {ok, Group} = kvs:get(group, GroupId),
            GE = Group#group.entries_count,
            kvs:put(Group#group{entries_count = GE+1}),
            {ok, Subs} = kvs:get(group_subs, {From, GroupId}),
            SE = Subs#group_subscription.user_posts_count,
            kvs:put(Subs#group_subscription{user_posts_count = SE+1})
    end,    

    self() ! {feed_refresh,Feed,20},
    {noreply, State};

handle_notice(["feed", "user", FeedOwner, "entry", EntryId, "add"] = Route,
              [From|_] = Message,
              #state{owner = WorkerOwner, feed = Feed, direct = Direct} = State) ->
    ?INFO("feed(~p): message: Owner=~p, Route=~p, Message=~p",
          [self(), WorkerOwner, Route, Message]),
    [From, Destinations, Desc, Medias] = Message,

    if
        %% user added message to own feed
        FeedOwner == From andalso FeedOwner == WorkerOwner->
            FilteredDst = [D || {_, group} = D <- Destinations],
            feed:add_entry(Feed, From, FilteredDst, EntryId, Desc, Medias,
                           {user, normal}), self() ! {feed_refresh,Feed,20};

        %% friend added message to public feed
        FeedOwner == From ->
            feed:add_entry(Feed, From, [], EntryId, Desc, Medias,
                           {user, normal}), self() ! {feed_refresh,Feed,20};

        %% direct message to worker owner
        FeedOwner == WorkerOwner ->
            feed:add_direct_message(Direct, From, [{FeedOwner, user}],
                                    EntryId, Desc, Medias), self() ! {direct_refresh,Direct,20};

        %% user sent direct message to friend, add copy to his direct feed
        From == WorkerOwner ->
            feed:add_direct_message(Direct, WorkerOwner, Destinations,
                                    EntryId, Desc, Medias), self() ! {direct_refresh,Direct,20};
        true ->
            ?INFO("not matched case in entry->add")
    end,
    
    {noreply, State};

% add/delete system message
handle_notice(["feed", "user", _FeedOwner, "entry", EntryId, "add_system"] = Route, 
              [From|_] = Message,
              #state{owner = WorkerOwner, feed = Feed, direct = _Direct} = State) ->
    ?INFO("feed(~p): system message: Owner=~p, Route=~p, Message=~p",
          [self(), WorkerOwner, Route, Message]),
    [From, _Destinations, Desc, Medias] = Message,

    feed:add_entry(Feed, From, [], EntryId, Desc, Medias, {user, system}),
    {noreply, State};

handle_notice(["feed", "group", GroupId, "entry", EntryId, "add_system"] = Route,
              [From|_] = Message,
              #state{owner = Owner, feed = Feed} = State) ->
    ?INFO("feed(~p): group system message: Owner=~p, Route=~p, Message=~p",
          [self(), Owner, Route, Message]),
    [From, _Destinations, Desc, Medias] = Message,
    feed:add_group_entry(Feed, From, [{GroupId, group}], EntryId,
                         Desc, Medias, {group, system}),
    {noreply, State};

handle_notice(["feed", "user", UId, "post_note"] = Route, Message, 
        #state{owner = Owner, feed = Feed} = State) ->
    ?INFO("feed(~p): post_note: Owner=~p, Route=~p, Message=~p", [self(), Owner, Route, Message]),
    Note = Message,
    Id = utils:uuid_ex(),
    feed:add_entry(Feed, UId, [], Id, Note, [], {user, system_note}),
    {noreply, State};

handle_notice(["feed", _, WhoShares, "entry", NewEntryId, "share"],
              #entry{entry_id = _EntryId, raw_description = Desc, media = Medias,
                     to = Destinations, from = From} = E,
              #state{feed = Feed, type = user} = State) ->
    %% FIXME: sharing is like posting to the wall
    ?INFO("share: ~p, WhoShares: ~p", [E, WhoShares]),
%    NewEntryId = utils:uuid_ex(),
    feed:add_shared_entry(Feed, From, Destinations, NewEntryId, Desc, Medias, {user, normal}, WhoShares),
    {noreply, State};

handle_notice(["feed", "group", _Group, "entry", EntryId, "delete"] = Route,
              Message,
              #state{owner = Owner, feed = Feed} = State) ->
    ?INFO("feed(~p): remove entry: Owner=~p, Route=~p, Message=~p",
          [self(), Owner, Route, Message]),
    %% all group subscribers shold delete entry from their feeds
    feed:remove_entry(Feed, EntryId),
    self() ! {feed_refresh,Feed,20},
    {noreply, State};

handle_notice(["feed", _Type, EntryOwner, "entry", EntryId, "delete"] = Route,
              Message,
              #state{owner = Owner, feed=Feed, direct=Direct} = State) ->
    case {EntryOwner, Message} of
        %% owner of the antry has deleted entry, we will delete it too
        {_, [EntryOwner|_]} ->
            ?INFO("feed(~p): remove entry: Owner=~p, Route=~p, Message=~p",
                  [self(), Owner, Route, Message]),
            kvs_feed:remove_entry(Feed, EntryId),
            kvs_feed:remove_entry(Direct, EntryId);
        %% we are owner of the entry - delete it
        {Owner, _} ->
            ?INFO("feed(~p): remove entry: Owner=~p, Route=~p, Message=~p",
                  [self(), Owner, Route, Message]),
            kvs_feed:remove_entry(Feed, EntryId),
            kvs_feed:remove_entry(Direct, EntryId);
        %% one of the friends has deleted some entry from his feed. Ignore
        _ ->
            ok
    end,
    self() ! {feed_refresh, State#state.feed,20},
    {noreply, State};

handle_notice(["feed", _Type, _EntryOwner, "entry", EntryId, "edit"] = Route,
              Message,
              #state{owner = Owner, feed=Feed} = State) ->
    [NewDescription|_] = Message,
    ?INFO("feed(~p): edit: Owner=~p, Route=~p, Message=~p",
          [self(), Owner, Route, Message]),

    %% edit entry in all feeds
    kvs_feed:edit_entry(Feed, EntryId, NewDescription),

    {noreply, State};

handle_notice(["feed", _Type, _EntryOwner, "comment", CommentId, "add"] = Route,
              Message,
              #state{owner = Owner, feed=Feed} = State) ->
    [From, EntryId, ParentComment, Content, Medias] = Message,

    ?INFO("feed(~p): add comment: Owner=~p, Route=~p, Message=~p",
          [self(), Owner, Route, Message]),
    kvs_feed:entry_add_comment(Feed, From, EntryId, ParentComment, CommentId, Content, Medias),
    {noreply, State};

handle_notice(["feed", "user", UId, "count_entry_in_statistics"] = Route, 
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): count_entry_in_statistics: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    case kvs:get(user_etries_count, UId) of
        {ok, UEC} -> 
            kvs:put(UEC#user_etries_count{
                entries = UEC#user_etries_count.entries+1
            }),
            kvs_users:attempt_active_user_top(UId, UEC#user_etries_count.entries+1);
        {error, notfound} ->
            kvs:put(#user_etries_count{
                user_id = UId,
                entries = 1
            }),
            kvs_users:attempt_active_user_top(UId, 1)
    end,
    {noreply, State};

handle_notice(["feed", "user", UId, "count_comment_in_statistics"] = Route, 
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): count_comment_in_statistics: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    case kvs:get(user_etries_count, UId) of
        {ok, UEC} -> 
            kvs:put(UEC#user_etries_count{
                comments = UEC#user_etries_count.comments+1
            });
        {error, notfound} ->
            kvs:put(#user_etries_count{
                user_id = UId,
                comments = 1
            })
    end,
    {noreply, State};

handle_notice(["likes", _, _, "add_like"] = Route,  % _, _ is here beacause of the same message used for comet update
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): add_like: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {UId, E} = Message,
    {EId, FId} = E#entry.id,
    feed:add_like(FId, EId, UId),
    {noreply, State};

handle_notice(Route, Message, State) -> error_logger:info_msg("Unknown FEEDS notice").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

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





% @spec entry_by_id(term()) -> {ok, #entry{}} | {error, not_found}.
entry_by_id(EntryId) -> kvs:get(entry, EntryId).


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

feed_direct_messages(_FId, Page, PageAmount, CurrentUser, CurrentFId) ->
    Page, PageAmount, CurrentUser, CurrentFId,
    [].

