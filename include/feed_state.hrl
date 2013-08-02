-record(state, {
        owner = "feed_owner",
        type :: user | group | system | product,
        feed,
        direct,
        cached_feed,
        cached_direct,
        cached_friends,
        cached_groups }).

-record(product_state, {
        owner = "feed_owner",
        type :: product,
        feed,
        blog,
        features,
        specs,
        gallery,
        videos,
        bundles,
        callback=feed_server_api}).

