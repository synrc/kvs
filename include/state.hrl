-ifndef(KVS_STATE_HRL).
-define(KVS_STATE_HRL, true).

-record(state, {
        owner = "feed_owner",
        type :: user | group | system | product,
        feeds = [],
        callback,
        cached_feeds=[]}).

-endif.
