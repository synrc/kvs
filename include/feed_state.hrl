-record(state, {
        owner = "feed_owner",
        type :: user | group | system | product,
        feed,
        direct,
        blog,
        features,
        specs,
        gallery,
        videos,
        bundles,
        products,
        callback=feed_server_api,  % tmp field\part of behaviour callback state
        cached_feed,
        cached_direct,
        cached_friends,
        cached_groups }).
