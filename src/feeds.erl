-module(feeds).
-compile(export_all).
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
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
        {ok, UEC} -> 
            UEC#user_etries_count.entries;
        {error, notfound} ->
            0
    end.

get_comments_count(Uid) ->
    case kvs:get(user_etries_count, Uid) of
        {ok, UEC} -> 
            UEC#user_etries_count.comments;
        {error, notfound} ->
            0
    end.

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
