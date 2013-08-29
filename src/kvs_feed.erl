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
-define(CACHED_ENTRIES, 20).

create() ->
    FId = kvs:next_id("feed", 1),
    ok = kvs:put(#feed{id = FId} ),
    FId.

comments_count(user,  Uid) -> case kvs:get(user_etries_count, Uid) of {error,_} -> 0; {ok, UEC} -> UEC#user_etries_count.comments end;
comments_count(entry, Eid) -> case kvs:get(entry, Eid) of {error,_} -> 0; {ok, E} -> comments_count([E],0) end;
comments_count([], Acc) -> Acc;
comments_count([E|T], Acc) ->
    C = case lists:keyfind(comments, 1, element(#iterator.feeds, E)) of false -> 0;
    {_, Fid} -> case kvs:get(feed, Fid) of {error,_} -> 0;
        {ok, Feed } -> Feed#feed.entries_count + comments_count(kvs:entries(Feed, comment), 0) end end,
    comments_count(T,  C + Acc).

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
        {error, _} ->
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
        {error, _} ->
            kvs:put(#user_likes{
                user_id = Uid,                
                one_like_head = Write_one_like(undefined),
                total_count = 1
            })
    end.

entries_count(Uid) ->
    case kvs:get(user_etries_count, Uid) of
        {ok, UEC} -> UEC#user_etries_count.entries;
        {error, _} -> 0 end.

edit_entry(FeedId, EId, NewDescription) ->
    case kvs:get(entry,{EId, FeedId}) of
        {ok, OldEntry} ->
            NewEntryRaw =  OldEntry#entry{description = NewDescription},
            NewEntry = feedformat:format(NewEntryRaw),
            kvs:put(NewEntry);
        {error, Reason}-> {error, Reason} end.

like_list(undefined) -> [];
like_list(Id) -> {ok, OneLike} = kvs:get(one_like, Id), [OneLike] ++ like_list(OneLike#one_like.next).
like_list(undefined, _) -> [];
like_list(_, 0) -> [];
like_list(Id, N) -> {ok, OneLike} = kvs:get(one_like, Id), [OneLike] ++ like_list(OneLike#one_like.next, N-1).

entry_likes(Entry_id) ->
    case kvs:get(entry_likes, Entry_id) of
        {ok, Likes} -> like_list(Likes#entry_likes.one_like_head);
        {error, _} -> [] end.

entry_likes_count(Entry_id) ->
    case kvs:get(entry_likes, Entry_id) of
        {ok, Likes} -> Likes#entry_likes.total_count;
        {error, _} -> 0 end.

user_likes_count(UserId) ->
    case kvs:get(user_likes, UserId) of
        {ok, Likes} -> Likes#user_likes.total_count;
        {error, _} -> 0 end.

user_likes(UserId) ->
    case kvs:get(user_likes, UserId) of
        {ok, Likes} -> like_list(Likes#user_likes.one_like_head);
        {error, _} -> [] end.

user_likes(UserId, {Page, PageAmount}) ->
    case kvs:get(user_likes, UserId) of
        {ok, Likes} -> lists:nthtail((Page-1)*PageAmount, like_list(Likes#user_likes.one_like_head, PageAmount*Page));
        {error, _} -> [] end.

%% MQ API

handle_notice([kvs_feed, _, Owner, entry, Eid, add],
              [#entry{feed_id=Fid}=Entry|_],
              #state{owner=Owner} = S) ->
    case lists:keyfind(Fid,2, S#state.feeds) of false -> skip;
      {_,_} ->
        EntryId = case Eid of new -> kvs:uuid(); _-> Eid end,
        E = Entry#entry{id = {EntryId, Fid}, entry_id = EntryId, feeds=[comments]},
        kvs:add(E) end,
    {noreply, S};

handle_notice([kvs_feed,_, Owner, entry, {_, Fid}, edit],
              #entry{entry_id=Eid}=Entry,
              #state{owner=Owner, feeds=Feeds}=S) ->

  case lists:keyfind(Fid,2,Feeds) of false -> skip;
    {_,_} -> case kvs:get(entry, {Eid, Fid}) of {error, not_found}-> skip; 
        {ok, E} -> 
          error_logger:info_msg("kvs_feed => Entry ~p updated in feed ~p", [Eid, Fid]),
          kvs:put(E#entry{description=Entry#entry.description,
                          title = Entry#entry.title,
                          media = Entry#entry.media,
                          etc   = Entry#entry.etc,
                          type  = Entry#entry.type}) end end,

  {noreply, S};

handle_notice([kvs_feed,_, Owner, entry, {_,Fid}, delete],
              [#entry{id=Id, entry_id=Eid},_], #state{owner=Owner, feeds=Feeds} = State) ->

  case lists:keyfind(Fid,2,Feeds) of false -> skip;
    {_,_} -> error_logger:info_msg("REMOVE from FID ~p", [Fid]),kvs:remove(entry, Id) end,
  %    self() ! {feed_refresh, FeedId, ?CACHED_ENTRIES};
  {noreply, State};

handle_notice([kvs_feed, entry, {Eid, FeedId}, comment, Cid, add],
              [From, Parent, Content, Medias, _, _],
              #state{owner=Owner, feeds=Feeds} = State) ->

    HasFeed = lists:keyfind(FeedId,2,Feeds) /= false,
    if HasFeed -> [begin
        {_, CFid} = lists:keyfind(comments, 1, E#entry.feeds),
        FeedId2 = case Parent of undefined -> CFid;
        Id -> case kvs:get(comment, {Parent, {E#entry.entry_id, E#entry.feed_id}}) of {error, not_found} -> CFid;
            {ok, C} -> {_, PCFid} = lists:keyfind(comments, 1, C#comment.feeds), PCFid end end,

        EntryId = E#entry.entry_id,
        FullId = {Cid, {EntryId, E#entry.feed_id}},
        User = From,

        Comment = #comment{id = FullId,
                     from = User,
                     comment_id = Cid,
                     entry_id = EntryId,
                     feed_id =  FeedId2, % entry commens or parent comment comments
                     content = Content,
                     media = Medias,
                     created = now(),
                     feeds = [comments]},
        kvs:add(Comment)

        end || E <- kvs:all_by_index(entry, entry_id, Eid)]; true -> skip end,

    {noreply, State};

handle_notice(["kvs_feed", "user", UId, "count_entry_in_statistics"] = Route, 
    Message, #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("queue_action(~p): count_entry_in_statistics: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    case kvs:get(user_etries_count, UId) of
        {ok, UEC} -> 
            kvs:put(UEC#user_etries_count{entries = UEC#user_etries_count.entries+1 }),
            kvs_users:attempt_active_user_top(UId, UEC#user_etries_count.entries+1);
        {error, _} ->
            kvs:put(#user_etries_count{user_id = UId, entries = 1 }),
            kvs_users:attempt_active_user_top(UId, 1) end,
    {noreply, State};

handle_notice(["kvs_feed", "user", UId, "count_comment_in_statistics"] = Route, 
    Message, #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("queue_action(~p): count_comment_in_statistics: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    case kvs:get(user_etries_count, UId) of
        {ok, UEC} -> kvs:put(UEC#user_etries_count{comments = UEC#user_etries_count.comments+1 });
        {error, _} -> kvs:put(#user_etries_count{ user_id = UId, comments = 1 }) end,
    {noreply, State};

handle_notice(["kvs_feed","likes", _, _, "add_like"] = Route,  % _, _ is here beacause of the same message used for comet update
    Message, #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("queue_action(~p): add_like: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {UId, E} = Message,
    {EId, FId} = E#entry.id,
    kvs_feed:add_like(FId, EId, UId),
    {noreply, State};

handle_notice(_Route, _Message, State) -> 
  %error_logger:error_msg("~p ===> Unknown FEED notice ~p", [State#state.owner, Route]), 
  {noreply, State}.
