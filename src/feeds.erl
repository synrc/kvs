-module(feeds).
-compile(export_all).
-include("feeds.hrl").
-include("users.hrl").
-include("groups.hrl").
-include("log.hrl").

create() ->
    FId = store:next_id("feed", 1),
    ok = store:put(#feed{id = FId} ),
    FId.

add_direct_message(FId, User, Desc) -> add_direct_message(FId, User, utils:uuid_ex(), Desc).
add_direct_message(FId, User, EntryId, Desc) -> add_direct_message(FId, User, undefined, EntryId, Desc, []).
add_direct_message(FId, User, To, EntryId, Desc, Medias) -> store:feed_add_direct_message(FId, User, To, EntryId, Desc, Medias).
add_group_entry(FId, User, EntryId, Desc, Medias) -> store:feed_add_entry(FId, User, EntryId, Desc, Medias).
add_group_entry(FId, User, To, EntryId, Desc, Medias, Type) -> store:feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, "").
add_entry(FId, User, EntryId, Desc) -> add_entry(FId, User, EntryId, Desc, []).
add_entry(FId, User, EntryId, Desc, Medias) -> store:feed_add_entry(FId, User, EntryId, Desc, Medias).
add_entry(FId, User, To, EntryId, Desc, Medias, Type) -> store:feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, "").
add_shared_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy) -> store:feed_add_entry(FId, User, To, EntryId, Desc, Medias, Type, SharedBy).

add_like(Fid, Eid, Uid) ->
    Write_one_like = fun(Next) ->
        Self_id = store:next_id("one_like", 1),   
        store:put(#one_like{    % add one like
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
    case store:get(entry_likes, Eid) of
        {ok, ELikes} -> 
            store:put(ELikes#entry_likes{
                one_like_head = Write_one_like(ELikes#entry_likes.one_like_head), 
                total_count = ELikes#entry_likes.total_count + 1
            });
        {error, notfound} ->
            store:put(#entry_likes{
                entry_id = Eid,                
                one_like_head = Write_one_like(undefined),
                total_count = 1
            })
    end,
    % add user - like
    case store:get(user_likes, Uid) of
        {ok, ULikes} -> 
            store:put(ULikes#user_likes{
                one_like_head = Write_one_like(ULikes#user_likes.one_like_head),
                total_count = ULikes#user_likes.total_count + 1
            });
        {error, notfound} ->
            store:put(#user_likes{
                user_id = Uid,                
                one_like_head = Write_one_like(undefined),
                total_count = 1
            })
    end.

% statistics

get_entries_count(Uid) ->
    case store:get(user_etries_count, Uid) of
        {ok, UEC} -> 
            UEC#user_etries_count.entries;
        {error, notfound} ->
            0
    end.

get_comments_count(Uid) ->
    case store:get(user_etries_count, Uid) of
        {ok, UEC} -> 
            UEC#user_etries_count.comments;
        {error, notfound} ->
            0
    end.

get_feed(FId) -> store:get(feed, FId).
get_entries_in_feed(FId) -> store:entries_in_feed(FId).
get_entries_in_feed(FId, Count) -> store:entries_in_feed(FId, Count).
get_entries_in_feed(FId, StartFrom, Count) -> store:entries_in_feed(FId, StartFrom, Count).
get_direct_messages(FId, Count) -> store:entries_in_feed(FId, undefined, Count).
get_direct_messages(FId, StartFrom, Count) -> store:entries_in_feed(FId, StartFrom, Count).
get_entries_in_feed(FId, StartFrom, Count, FromUserId)-> Entries = store:entries_in_feed(FId, StartFrom, Count),
    [E || #entry{from = From} = E <- Entries, From == FromUserId].

create_message(Table) ->
    EId = store:next_id("entry", 1),
    #entry{id = {EId, system_info},
        entry_id = EId,
        from = system,
        type = {system, new_table},
        created_time = now(),
        description = Table}.


remove_entry(FeedId, EId) ->
    {ok, #feed{top = TopId} = Feed} = get_feed(FeedId),

    case store:get(entry, {EId, FeedId}) of
        {ok, #entry{prev = Prev, next = Next}}->
            ?INFO("P: ~p, N: ~p", [Prev, Next]),
            case store:get(entry, Next) of
                {ok, NE} -> store:put(NE#entry{prev = Prev});
                _ -> ok
            end,
            case store:get(entry, Prev) of
                {ok, PE} -> store:put(PE#entry{next = Next});
                _ -> ok
            end,

            case TopId of
                {EId, FeedId} -> store:put(Feed#feed{top = Prev});
                _ -> ok
            end;
        {error, notfound} -> ?INFO("Not found"), ok
    end,
    store:delete(entry, {EId, FeedId}).

edit_entry(FeedId, EId, NewDescription) ->
    case store:entry_by_id({EId, FeedId}) of
        {ok, OldEntry} ->
            NewEntryRaw =  OldEntry#entry{description = NewDescription,
                                          raw_description = NewDescription},
            NewEntry = feedformat:format(NewEntryRaw),
            store:put(NewEntry);
        {error, notfound}->
            {error, notfound}
    end.

remove_entry_comments(FId, EId) ->
    AllComments = store:comments_by_entry(FId, EId),
    [begin
          store:delete(comment, ID),
          remove_media(M)
     end || #comment{id = ID, media = M} <- AllComments].

entry_add_comment(FId, User, EntryId, ParentComment, CommentId, Content, Medias) ->
     case store:entry_by_id({EntryId, FId}) of
         {ok, _E} ->
             store:add_comment(FId, User, EntryId, ParentComment, CommentId, Content, Medias);
         _ ->
             ok
     end.

remove_media([]) -> ok;
remove_media([#media{url=undefined, thumbnail_url=undefined}|T]) -> remove_media(T);
remove_media([#media{url=undefined, thumbnail_url=TUrl}|T]) -> file:delete(?ROOT ++ TUrl), remove_media(T);
remove_media([#media{url=Url, thumbnail_url=undefined}|T]) -> file:delete(?ROOT ++ Url), remove_media(T);
remove_media([#media{url=Url, thumbnail_url=TUrl}|T]) -> file:delete(?ROOT ++ Url),file:delete(?ROOT ++ TUrl), remove_media(T).


get_one_like_list(undefined) -> [];
get_one_like_list(Id) -> {ok, OneLike} = store:get(one_like, Id),
    [OneLike] ++ get_one_like_list(OneLike#one_like.next).

get_entries_likes(Entry_id) ->
    case store:get(entry_likes, Entry_id) of
        {ok, Likes} -> get_one_like_list(Likes#entry_likes.one_like_head);
        {error, notfound} -> []
    end.

get_entries_likes_count(Entry_id) ->
    case store:get(entry_likes, Entry_id) of
        {ok, Likes} ->
            Likes#entry_likes.total_count;
        {error, notfound} -> 0
    end.

get_user_likes_count(UserId) ->
    case store:get(user_likes, UserId) of
        {ok, Likes} -> Likes#user_likes.total_count;
        {error, notfound} -> 0
    end.

get_user_likes(UserId) ->
    case store:get(user_likes, UserId) of
        {ok, Likes} -> get_one_like_list(Likes#user_likes.one_like_head);
        {error, notfound} -> []
    end.

get_one_like_list(undefined, _) -> [];
get_one_like_list(_, 0) -> [];
get_one_like_list(Id, N) -> {ok, OneLike} = store:get(one_like, Id),
    [OneLike] ++ get_one_like_list(OneLike#one_like.next, N-1).

get_user_likes(UserId, {Page, PageAmount}) ->
    case store:get(user_likes, UserId) of
        {ok, Likes} -> lists:nthtail((Page-1)*PageAmount, get_one_like_list(Likes#user_likes.one_like_head, PageAmount*Page));
        {error, notfound} -> []
    end.

% we have same in nsm_user? Why?
is_subscribed_user(UserUidWho, UserUidWhom) -> nsm_users:is_user_subscr(UserUidWho, UserUidWhom).
user_subscription_count(UserUid) -> length(nsm_users:list_subscr(UserUid)).
user_friends_count(UserUid) -> length(nsm_users:list_subscr_me(UserUid)).

get_comments_entries(UserUid, _, _Page, _PageAmount) ->
    Pids = [Eid || #comment{entry_id=Eid} <- store:select(comment,
        fun(#comment{author_id=Who}) when Who=:=UserUid ->true;(_)->false end)],
    %?PRINT({"GCE pids length: ", length(Pids)}),
    lists:flatten([store:select(entry,[{where, fun(#entry{entry_id=ID})-> ID=:=Pid end},
        {order, {1, descending}},{limit, {1,1}}]) || Pid <- Pids]).

get_my_discussions(_FId, Page, PageAmount, UserUid) ->
    _Offset= case (Page-1)*PageAmount of
        0 -> 1
        ;M-> M
    end,
    Pids = [Eid || #comment{entry_id=Eid} <- store:select(comment,
        fun(#comment{author_id=Who}) when Who=:=UserUid ->true;(_)->false end)],
    lists:flatten([store:select(entry,[{where, fun(#entry{entry_id=ID})-> ID=:=Pid end},
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
