-module(kvs_feed).
-author('Maxim Sokhatsky').
-author('Andrii Zadorozhnii').
-copyright('Synrc Research Center, s.r.o.').
-compile(export_all).
-include_lib("kvs/include/kvs.hrl").
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feed_state.hrl").
-define(CACHED_ENTRIES, 20).

init(Backend) ->
    ?CREATE_TAB(feed),
    ?CREATE_TAB(entry),
    ?CREATE_TAB(comment),
    Backend:add_table_index(entry, feed_id),
    Backend:add_table_index(entry, entry_id),
    Backend:add_table_index(entry, from),
    Backend:add_table_index(comment, entry_id),
    Backend:add_table_index(comment, author_id),
    ok.

create() ->
    FId = kvs:next_id("feed", 1),
    ok = kvs:put(#feed{id = FId} ),
    FId.

comments_count(user,  Uid) -> case kvs:get(user_etries_count, Uid) of {error,_} -> 0; {ok, UEC} -> UEC#user_etries_count.comments end;
comments_count(entry, Eid) -> case kvs:get(entry, Eid) of {error,_} -> 0; {ok, E} -> comments_count([E],0) end;
comments_count(product, Pid)->case kvs:get(product, Pid) of {error,_}->0; {ok, P} -> comments_count([P], 0) end;
comments_count([], Acc) -> Acc;
comments_count([E|T], Acc) ->
    C = case lists:keyfind(comments, 1, element(#iterator.feeds, E)) of false -> 0;
    {_, Fid} -> case kvs:get(feed, Fid) of {error,_} -> 0;
        {ok, Feed } -> Feed#feed.entries_count 
            + comments_count(kvs:entries(Feed, comment, undefined), 0) end end,
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
              [#entry{feed_id=Fid}=Entry],
              #state{owner=Owner} = S) ->
    case lists:keyfind(Fid,2, S#state.feeds) of false -> skip;
    {_,_} -> add_entry(Eid,Fid,Entry) end,

    {noreply, S};

handle_notice([kvs_feed,_, Owner, entry, {Eid, FeedName}, edit],
              [#entry{}=Entry],
              #state{owner=Owner, feeds=Feeds}=S) ->
    case lists:keyfind(FeedName,1,Feeds) of false -> skip; {_,Fid}-> update_entry(Eid,Fid,Entry) end,

    {noreply, S};

handle_notice([kvs_feed,_, Owner, entry, Eid, edit],
              [#entry{feed_id=Fid}=Entry],
              #state{owner=Owner, feeds=Feeds}=S) ->
    case lists:keyfind(Fid, 2, Feeds) of false -> skip;
    {_,_} -> update_entry(Eid,Fid,Entry) end,

    {noreply, S};

handle_notice([kvs_feed, Owner, entry, delete],
              [#entry{id=Id,feed_id=Fid}=E],
              #state{owner=Owner, feeds=Feeds}=State) ->
    error_logger:info_msg("DELETE"),
    case lists:keyfind(Fid,2,Feeds) of false -> ok;
    _ ->
        error_logger:info_msg("[kvs_feed] => Remove entry ~p from feed ~p", [Id, Fid]),
        Removed = case kvs:remove(entry, Id) of {error,Er}->{error, Er}; ok -> E end,
        msg:notify([kvs_feed, entry, Id, deleted], [Removed]) end,

    {noreply,State};

handle_notice([kvs_feed, Owner, delete],
              [#entry{entry_id=Eid}=E],
              #state{owner=Owner}=State) ->
    error_logger:info_msg("[kvs_feed] Delete all entries ~p ~p", [E#entry.entry_id, Owner]),

    [msg:notify([kvs_feed, To, entry, delete],[Ed])
        || #entry{to={_, To}}=Ed <- kvs:all_by_index(entry, entry_id, Eid)],

    Fid = ?FEED(entry),
    Removed = case kvs:remove(entry,{Eid, Fid}) of {error,X} -> {error,X}; ok -> E#entry{id={Eid,Fid},feed_id=Fid} end,
    msg:notify([kvs_feed, entry, {Eid, Fid}, deleted], [Removed]),

    {noreply, State};

handle_notice([kvs_feed,_,Owner, comment, Cid, add],
              [#comment{id={Cid, {_, EFid}, _}}=C],
              #state{owner=Owner, feeds=Feeds} = S) ->
    case lists:keyfind(EFid,2,Feeds) of false -> skip;
    {_,_}-> add_comment(C) end,

    {noreply, S};

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

add_comment(C) ->
    error_logger:info_msg("[kvs_feed] Add comment ~p ~p", [C#comment.id, C#comment.feed_id]),
    Added = case kvs:add(C#comment{feeds=[comments]}) of {error, E} -> {error, E}; {ok, Cm} -> Cm end,
    msg:notify([kvs_feed, comment, C#comment.id, added], [Added]).

add_entry(Eid,Fid,Entry) ->
    error_logger:info_msg("[kvs_feed] => Add entry ~p to feed ~p.", [Eid, Fid]),
    E = Entry#entry{id = {Eid, Fid}, entry_id = Eid, feeds=[comments]},
    Added = case kvs:add(E) of {error, Err}-> {error,Err}; {ok, En} -> En end,
    msg:notify([kvs_feed, entry, {Eid, Fid}, added], [Added]).

update_entry(Eid,Fid,Entry) ->
    case kvs:get(entry, {Eid,Fid}) of false -> skip;
    {ok, E} ->
        error_logger:info_msg("[kvs_feed] => Update entry ~p in ~p", [Eid, Fid]),
        Upd = E#entry{description=Entry#entry.description,
                      title = Entry#entry.title,
                      media = Entry#entry.media,
                      etc   = Entry#entry.etc,
                      type  = Entry#entry.type},
        kvs:put(Upd),
        msg:notify([kvs_feed, entry, {Eid, Fid}, updated], [Upd]) end.

