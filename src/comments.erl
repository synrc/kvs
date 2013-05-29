-module(comments).
-include("feeds.hrl").
-compile(export_all).

add(EId, User, Value) ->  add(EId, User, Value, []).
add(EId, User, Value, Medias) ->
    CId = store:next_id("comment"),
    R = #comment{id = {CId, EId}, comment_id = CId, entry_id = EId,
                 content = Value, author_id = User, media = Medias, create_time = now() },
    case store:put(R) of
        ok -> {ok, R};
        A -> A end.

select_by_entry_id(EntryId) ->
    store:comments_by_entry(EntryId).
