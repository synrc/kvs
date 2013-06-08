-module(kvs_comment).
-copyright('Synrc Research Center s.r.o.').
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/config.hrl").
-compile(export_all).

add(FId, User, EntryId, ParentComment, CommentId, Content, Medias) ->
     case kvs:get(entry,{EntryId, FId}) of
         {ok, _E} -> add(FId, User, EntryId, ParentComment, CommentId, Content, Medias, dont_check);
         _ -> ok end.

add(FId, User, EntryId, ParentComment, CommentId, Content, Medias, _) ->
    FullId = {CommentId, {EntryId, FId}},

    Prev = case ParentComment of
        undefined ->
            {ok, Entry} = kvs:get(entry,{EntryId, FId}),
            {PrevC, E} = case Entry#entry.comments of
                        undefined -> {undefined, Entry#entry{comments_rear = FullId}};
                        Id -> {ok, PrevTop} = kvs:get(comment, Id),
                              kvs:put(PrevTop#comment{next = FullId}),
                              {Id, Entry} end,
            kvs:put(E#entry{comments=FullId}),
            PrevC;
        _ ->
            {ok, Parent} = kvs:get(comment, {{EntryId, FId}, ParentComment}),
            {PrevC, CC} = case Parent#comment.comments of
                        undefined -> {undefined, Parent#comment{comments_rear = FullId}};
                        Id -> {ok, PrevTop} = kvs:get(comment, Id),
                              kvs:put(PrevTop#comment{next = FullId}),
                              {Id, Parent} end,
            kvs:put(CC#comment{comments = FullId}),
            PrevC end,

    Comment = #comment{id = FullId,
                       author_id = User,
                       comment_id = CommentId,
                       entry_id = EntryId,
                       content = Content,
                       media = Medias,
                       creation_time = now(),
                       prev = Prev,
                       next = undefined},

    kvs:put(Comment),
    {ok, Comment}.


read_comments(undefined) -> [];
read_comments([#comment{comments = C} | Rest]) -> [read_comments(C) | read_comments(Rest)];
read_comments(C) -> kvs:traversal(comment, #comment.next, C, all).

feed_comments({EId, FId}) ->
    case kvs:get(entry,{EId, FId}) of
        {ok, #entry{comments_rear = undefined}} -> [];
        {ok, #entry{comments_rear = First}} -> lists:flatten(read_comments(First));
        _ -> [] end.

author_comments(Who) -> DBA=?DBA,DBA:author_comments(Who).

remove(FId, EId) ->
    AllComments = feed_comments({EId, FId}),
    [begin kvs:delete(comment, ID) end || #comment{id = ID, media = M} <- AllComments].

