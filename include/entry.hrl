-include("kvs.hrl").

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
        media = [],
        type = {user, normal}}).
