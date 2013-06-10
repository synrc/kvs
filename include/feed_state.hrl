-record(state, {
        owner = "feed_owner",
        type :: user | group | system | product,
        feed,
        direct,
        cached_feed,
        cached_direct,
        cached_friends,
        cached_groups }).

