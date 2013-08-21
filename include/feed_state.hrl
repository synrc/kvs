-record(state, {
        owner = "feed_owner",
        type :: user | group | system | product,
        feeds = [],
        callback,
        cached_feeds=[]}).
