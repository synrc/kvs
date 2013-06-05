-module(kvs_feed).
-author('Maxim Sokhatsky').
-author('Andrii Zadorozhnii').
-author('Alexander Kalenuk').
-copyright('Synrc Research Center, s.r.o.').
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

add_entry(FId, User, To, EntryId,Desc,Medias,Type,SharedBy) ->
    case kvs:get(entry,{EntryId, FId}) of
        {ok, _} -> ok;
        _ -> add_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy, dont_check) end.

add_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy, _) ->
    {ok,Feed} = kvs:get(feed, erlang:integer_to_list(FId)),

    Id = {EntryId, FId},
    Next = undefined,
    Prev = case Feed#feed.top of
               undefined -> undefined;
               X -> case kvs:get(entry, X) of
                       {ok, TopEntry} -> EditedEntry = TopEntry#entry{next = Id}, kvs:put(EditedEntry), TopEntry#entry.id;
                       {error,notfound} -> undefined end end,

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
                   {_, Reason} -> ?ERROR("feedformat error: ~p", [Reason]), Entry;
                   #entry{} = ME -> ME end,

    kvs:put(ModEntry),

    {ok, ModEntry}.

entry_traversal(undefined, _) -> [];
entry_traversal(_, 0) -> [];
entry_traversal(Next, Count)->
    case kvs:get(entry, Next) of
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
            [R | entry_traversal(Prev, Count1)]
    end.

entries(FeedId, undefined, PageAmount) ->
    case kvs:get(feed, FeedId) of
        {ok, O} -> entry_traversal(O#feed.top, PageAmount);
        {error, notfound} -> [] end;
entries(FeedId, StartFrom, PageAmount) ->
    case kvs:get(entry,{StartFrom, FeedId}) of
        {ok, #entry{prev = Prev}} -> entry_traversal(Prev, PageAmount);
        _ -> [] end.

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

entries_count(Uid) ->
    case kvs:get(user_etries_count, Uid) of
        {ok, UEC} -> UEC#user_etries_count.entries;
        {error, notfound} -> 0 end.

comments_count(Uid) ->
    case kvs:get(user_etries_count, Uid) of
        {ok, UEC} -> UEC#user_etries_count.comments;
        {error, notfound} -> 0 end.

remove_entry(FeedId, EId) ->
    {ok, #feed{top = TopId} = Feed} = kvs:get(feed,FeedId),
    case kvs:get(entry, {EId, FeedId}) of
        {ok, #entry{prev = Prev, next = Next}}->
            ?INFO("P: ~p, N: ~p", [Prev, Next]),
            case kvs:get(entry, Next) of {ok, NE} -> kvs:put(NE#entry{prev = Prev});  _ -> ok end,
            case kvs:get(entry, Prev) of {ok, PE} -> kvs:put(PE#entry{next = Next});  _ -> ok end,
            case TopId of {EId, FeedId} -> kvs:put(Feed#feed{top = Prev}); _ -> ok end;
        {error, notfound} -> ?INFO("Not found"), ok
    end,
    kvs:delete(entry, {EId, FeedId}).

edit_entry(FeedId, EId, NewDescription) ->
    case kvs:get(entry,{EId, FeedId}) of
        {ok, OldEntry} ->
            NewEntryRaw =  OldEntry#entry{description = NewDescription, raw_description = NewDescription},
            NewEntry = feedformat:format(NewEntryRaw),
            kvs:put(NewEntry);
        {error, notfound}-> {error, notfound} end.

like_list(undefined) -> [];
like_list(Id) -> {ok, OneLike} = kvs:get(one_like, Id), [OneLike] ++ like_list(OneLike#one_like.next).
like_list(undefined, _) -> [];
like_list(_, 0) -> [];
like_list(Id, N) -> {ok, OneLike} = kvs:get(one_like, Id), [OneLike] ++ like_list(OneLike#one_like.next, N-1).

entry_likes(Entry_id) ->
    case kvs:get(entry_likes, Entry_id) of
        {ok, Likes} -> like_list(Likes#entry_likes.one_like_head);
        {error, notfound} -> [] end.

entry_likes_count(Entry_id) ->
    case kvs:get(entry_likes, Entry_id) of
        {ok, Likes} -> Likes#entry_likes.total_count;
        {error, notfound} -> 0 end.

user_likes_count(UserId) ->
    case kvs:get(user_likes, UserId) of
        {ok, Likes} -> Likes#user_likes.total_count;
        {error, notfound} -> 0 end.

user_likes(UserId) ->
    case kvs:get(user_likes, UserId) of
        {ok, Likes} -> like_list(Likes#user_likes.one_like_head);
        {error, notfound} -> [] end.

user_likes(UserId, {Page, PageAmount}) ->
    case kvs:get(user_likes, UserId) of
        {ok, Likes} -> lists:nthtail((Page-1)*PageAmount, like_list(Likes#user_likes.one_like_head, PageAmount*Page));
        {error, notfound} -> [] end.

purge_feed(FeedId) ->
    {ok,Feed} = kvs:get(feed,FeedId),
    Removal = entry_traversal(Feed#feed.top, -1),
    [kvs:delete(entry,Id)||#entry{id=Id}<-Removal],
    kvs:put(Feed#feed{top=undefined}).

purge_unverified_feeds() ->
    [purge_feed(FeedId) || #user{feed=FeedId,status=S,email=E} <- kvs:all(user), E==undefined].

%% MQ API

handle_notice(["kvs_feed", "group", GroupId, "entry", EntryId, "add"] = Route, [From|_] = Message, #state{owner = Owner, feed = Feed} = State) ->
    ?INFO("feed(~p): group message: Owner=~p, Route=~p, Message=~p", [self(), Owner, Route, Message]),
    [From, _Destinations, Desc, Medias] = Message,
    kvs_feed:add_entry(Feed, From, [{GroupId, group}], EntryId, Desc, Medias, {group, direct}, ""),
    case Owner == GroupId of
        false -> ok;
        true ->
            {ok, Group} = kvs:get(group, GroupId),
            GE = Group#group.entries_count,
            kvs:put(Group#group{entries_count = GE+1}),
            {ok, Subs} = kvs:get(group_subscription, {From, GroupId}),
            SE = Subs#group_subscription.user_posts_count,
            kvs:put(Subs#group_subscription{user_posts_count = SE+1})
    end,
    self() ! {feed_refresh,Feed,20},
    {noreply, State};

handle_notice(["kvs_feed", "user", FeedOwner, "entry", EntryId, "add"] = Route,
    [From|_] = Message, #state{owner = WorkerOwner, feed = Feed, direct = Direct} = State) ->
    ?INFO("feed(~p): message: Owner=~p, Route=~p, Message=~p", [self(), WorkerOwner, Route, Message]),

    [From, Destinations, Desc, Medias] = Message,

    if
        %% user added message to own feed
        FeedOwner == From andalso FeedOwner == WorkerOwner->
            FilteredDst = [D || {_, group} = D <- Destinations],
            kvs_feed:add_entry(Feed, From, FilteredDst, EntryId, Desc, Medias, {user, normal},""),
            self() ! {feed_refresh,Feed,20};

        %% friend added message to public feed
        FeedOwner == From -> 
            kvs_feed:add_entry(Feed, From, [], EntryId, Desc, Medias, {user, normal},""),
            self() ! {feed_refresh,Feed,20};

        %% direct message to worker owner
        FeedOwner == WorkerOwner -> 
            kvs_feed:add_entry(Direct, From, [{FeedOwner, user}], EntryId, Desc, Medias, {user,direct}, ""),
            self() ! {direct_refresh,Direct,20};

        %% user sent direct message to friend, add copy to his direct feed
        From == WorkerOwner ->
            kvs_feed:add_entry(Direct, WorkerOwner, Destinations, EntryId, Desc, Medias, {user, direct}, ""),
            self() ! {direct_refresh,Direct,20};

        true -> ?INFO("not matched case in entry->add")
    end,

    {noreply, State};

handle_notice(["kvs_feed", "user", _FeedOwner, "entry", EntryId, "add_system"] = Route,
    [From|_] = Message, #state{owner = WorkerOwner, feed = Feed, direct = _Direct} = State) ->
    ?INFO("feed(~p): system message: Owner=~p, Route=~p, Message=~p", [self(), WorkerOwner, Route, Message]),
    [From, _Destinations, Desc, Medias] = Message,
    kvs_feed:add_entry(Feed, From, [], EntryId, Desc, Medias, {user, system}, ""),
    {noreply, State};

handle_notice(["kvs_feed", "group", GroupId, "entry", EntryId, "add_system"] = Route,
              [From|_] = Message, #state{owner = Owner, feed = Feed} = State) ->
    ?INFO("feed(~p): group system message: Owner=~p, Route=~p, Message=~p",
          [self(), Owner, Route, Message]),
    [From, _Destinations, Desc, Medias] = Message,
    kvs_feed:add_entry(Feed, From, [{GroupId, group}], EntryId, Desc, Medias, {group, system}, ""),
    {noreply, State};

handle_notice(["feed", "user", UId, "post_note"] = Route,
    Message, #state{owner = Owner, feed = Feed} = State) ->
     ?INFO("feed(~p): post_note: Owner=~p, Route=~p, Message=~p", [self(), Owner, Route, Message]),
    Note = Message,
    Id = utils:uuid_ex(),
    kvs_feed:add_entry(Feed, UId, [], Id, Note, [], {user, system_note}, ""),
    {noreply, State};

handle_notice(["kvs_feed", _, WhoShares, "entry", NewEntryId, "share"] = Route,
                #entry{entry_id = _EntryId, raw_description = Desc, media = Medias, to = Destinations,
                from = From} = E, #state{feed = Feed, type = user} = State) ->
    %% FIXME: sharing is like posting to the wall
    ?INFO("share: ~p, WhoShares: ~p", [E, WhoShares]),
    kvs_feed:add_entry(Feed, From, Destinations, NewEntryId, Desc, Medias, {user, normal}, WhoShares),
    {noreply, State};

handle_notice(["kvs_feed", "group", _Group, "entry", EntryId, "delete"] = Route,
              Message, #state{owner = Owner, feed = Feed} = State) ->
    ?INFO("feed(~p): remove entry: Owner=~p, Route=~p, Message=~p",
          [self(), Owner, Route, Message]),
    %% all group subscribers shold delete entry from their feeds
    kvs_feed:remove_entry(Feed, EntryId),
    self() ! {feed_refresh,Feed,20},
    {noreply, State};

handle_notice(["kvs_feed", _Type, EntryOwner, "entry", EntryId, "delete"] = Route,
              Message, #state{owner = Owner, feed=Feed, direct=Direct} = State) ->
    case {EntryOwner, Message} of
        %% owner of the antry has deleted entry, we will delete it too
        {_, [EntryOwner|_]} ->
            ?INFO("feed(~p): remove entry: Owner=~p, Route=~p, Message=~p", [self(), Owner, Route, Message]),
            kvs_feed:remove_entry(Feed, EntryId),
            kvs_feed:remove_entry(Direct, EntryId);
        %% we are owner of the entry - delete it
        {Owner, _} ->
            ?INFO("feed(~p): remove entry: Owner=~p, Route=~p, Message=~p", [self(), Owner, Route, Message]),
            kvs_feed:remove_entry(Feed, EntryId),
            kvs_feed:remove_entry(Direct, EntryId);
        %% one of the friends has deleted some entry from his feed. Ignore
        _ -> ok end,
    self() ! {feed_refresh, State#state.feed,20},
    {noreply, State};

handle_notice(["kvs_feed", _Type, _EntryOwner, "entry", EntryId, "edit"] = Route,
              Message, #state{owner = Owner, feed=Feed} = State) ->
    [NewDescription|_] = Message,
    ?INFO("feed(~p): edit: Owner=~p, Route=~p, Message=~p", [self(), Owner, Route, Message]),
    kvs_feed:edit_entry(Feed, EntryId, NewDescription),
    {noreply, State};

handle_notice(["kvs_feed", _Type, _EntryOwner, "comment", CommentId, "add"] = Route,
              Message, #state{owner = Owner, feed=Feed} = State) ->
    [From, EntryId, ParentComment, Content, Medias] = Message,
    ?INFO("feed(~p): add comment: Owner=~p, Route=~p, Message=~p", [self(), Owner, Route, Message]),
    kvs_comment:add(Feed, From, EntryId, ParentComment, CommentId, Content, Medias),
    {noreply, State};

handle_notice(["kvs_feed", "user", UId, "count_entry_in_statistics"] = Route, 
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): count_entry_in_statistics: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    case kvs:get(user_etries_count, UId) of
        {ok, UEC} -> 
            kvs:put(UEC#user_etries_count{entries = UEC#user_etries_count.entries+1 }),
            kvs_users:attempt_active_user_top(UId, UEC#user_etries_count.entries+1);
        {error, notfound} ->
            kvs:put(#user_etries_count{user_id = UId, entries = 1 }),
            kvs_users:attempt_active_user_top(UId, 1) end,
    {noreply, State};

handle_notice(["kvs_feed", "user", UId, "count_comment_in_statistics"] = Route, 
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): count_comment_in_statistics: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    case kvs:get(user_etries_count, UId) of
        {ok, UEC} -> kvs:put(UEC#user_etries_count{comments = UEC#user_etries_count.comments+1 });
        {error, notfound} -> kvs:put(#user_etries_count{ user_id = UId, comments = 1 }) end,
    {noreply, State};

handle_notice(["kvs_feed","likes", _, _, "add_like"] = Route,  % _, _ is here beacause of the same message used for comet update
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): add_like: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {UId, E} = Message,
    {EId, FId} = E#entry.id,
    kvs_feed:add_like(FId, EId, UId),
    {noreply, State};

handle_notice(Route, Message, State) -> error_logger:error_msg("Unknown FEED notice").
