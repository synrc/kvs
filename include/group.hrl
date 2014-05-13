-include("kvs.hrl").

-record(group,{?ITERATOR(feed, true),
        name,
        description,
        scope :: public | private,
        creator,
        created,
        owner,
        users_count = 0 :: integer(),
        entries_count = 0}).
