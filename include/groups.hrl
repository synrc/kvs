-include("types.hrl").
-include("kvs.hrl").

-record(group,{?ITERATOR(feed, true),
        name,
        description,
        scope :: public | private,
        creator,
        created,
        owner,
        users_count = 0 :: integer(),   % we have to store this, counting would be very expensive and this number is sufficient for sorting and stuff
        entries_count = 0}).

-record(group_subscription, {
        key,
        who,
        where,
        type,
        posts_count = 0 :: integer() % we need this for sorting and counting is expensive
        }).

-define(GROUP_EXCHANGE(GroupId), list_to_binary("group_exchange."++GroupId++".fanout")).
