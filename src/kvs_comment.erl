-module(kvs_comment).
-include("feeds.hrl").
-compile(export_all).

select_by_entry_id(EntryId) ->
    kvs:comments_by_entry(EntryId).

read_comments(undefined) -> [];
read_comments([#comment{comments = C} | Rest]) -> [read_comments(C) | read_comments(Rest)];
read_comments(C) -> kvs:traversal(comment, #comment.prev, C, all).

read_comments_rev(undefined) -> [];
read_comments_rev([#comment{comments = C} | Rest]) -> [read_comments_rev(C) | read_comments_rev(Rest)];
read_comments_rev(C) -> kvs:traversal(comment, #comment.next, C, all).

% @spec comment_by_id({{EntryId::term(), FeedId::term()}, CommentId::term()}) -> {ok, #comment{}}.
comment_by_id(CommentId) -> kvs:get(CommentId).

% @spec comments_by_entry(EId::{string(), term()}) -> [#comment{}].
comments_by_entry({EId, FId}) ->
    case kvs:entry_by_id({EId, FId}) of
        {ok, #entry{comments_rear = undefined}} ->
            [];
        {ok, #entry{comments_rear = First}} ->
            lists:flatten(read_comments_rev(First));
        _ ->
            []
    end.


add(FId, User, EntryId, ParentComment, CommentId, Content, Medias) ->
    FullId = {CommentId, {EntryId, FId}},

    Prev = case ParentComment of
        undefined ->
            {ok, Entry} = kvs:entry_by_id({EntryId, FId}),
            {PrevC, E} = case Entry#entry.comments of
                        undefined ->
                            {undefined, Entry#entry{comments_rear = FullId}};
                        Id ->
                            {ok, PrevTop} = kvs:get(comment, Id),
                            kvs:put(PrevTop#comment{next = FullId}),
                            {Id, Entry}
                   end,

            kvs:put(E#entry{comments=FullId}),
            PrevC;

        _ ->
            {ok, Parent} = kvs:get(comment, {{EntryId, FId}, ParentComment}),
            {PrevC, CC} = case Parent#comment.comments of
                        undefined ->
                            {undefined, Parent#comment{comments_rear = FullId}};
                        Id ->
                            {ok, PrevTop} = kvs:get(comment, Id),
                            kvs:put(PrevTop#comment{next = FullId}),
                            {Id, Parent}
                    end,
            kvs:put(CC#comment{comments = FullId}),
            PrevC
    end,
    Comment = #comment{id = FullId,
                       author_id = User,
                       comment_id = CommentId,
                       entry_id = EntryId,
                       raw_content = Content,
                       content = Content,
                       media = Medias,
                       create_time = now(),
                       prev = Prev,
                       next = undefined
                      },
    kvs:put(Comment),
    {ok, Comment}.

