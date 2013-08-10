-record(state, {
        owner = "feed_owner",
        type :: user | group | system | product,
        feeds = [],
        callback=feed_server_api,  % tmp field\part of behaviour callback state
        cached_feeds=[]}).
