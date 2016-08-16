-ifndef(FEED_HRL).
-define(FEED_HRL, true).

-include("kvs.hrl").

-record(feed, {?CONTAINER, aclver=[]}).

-endif.
