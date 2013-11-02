-include("kvs.hrl").
-record(feed, {?CONTAINER, aclver}).

-record(entry, {?ITERATOR(feed), % {entry_id, feed_id}
        entry_id,
        from,
        to,
        title,
        description,
        created,
        hidden,
        access,
        shared,
        starred,
        deleted,
%        likes,
%        likes_count,
        media = [],
        etc,       %% field to link additional info
        type = {user, normal}}).

-record(media, {
        id,
        title :: iolist(),
        width,
        height,
        html :: iolist(),
        url :: iolist(),
        version,
        thumbnail_url :: iolist(),
        type :: {atom(), atom() | string()},
        thumbnail_height}).

-record(comment, {?ITERATOR(feed), % {comment_id, entry_id, feed_id}
        comment_id,
        entry_id,
        content,
        from,
        created,
        media = [],
        parent}).

-record(entry_views, {?CONTAINER}).
-record(user_view,   {?ITERATOR(entry_views), user, created}).
-record(like, {?ITERATOR(feed), user_id, entry_id, created}).

-record(entry_likes, {
        entry_id,       % this is a general entry_id. Every same entry in different feeds has the same id
        one_like_head,  % this is a head for linked list of {user, time} tupples
        total_count     % it's easier to keep it than count
        }).

-record(user_likes, {
        user_id,
        one_like_head,
        total_count
        }).


-record(one_like, {
        id,              % just a number
        user_id,        % who likes
        entry_id,       % what
        feed_id,        % where
        created_time,   % when
        next
        }).

-record(hidden_feed, {id}).

% Statistics. We have to keep count of user entries and comments. 
% Gathering it the old way will work very ineffective with more users to come.
% And comments from user record are somehow always undefined. Either it fails, or it is used somewhere else

-record(user_etries_count, {
        user_id,    % user id
        entries = 0,    % number of entries
        comments = 0   % number of comments
        }).
