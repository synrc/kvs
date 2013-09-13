-ifndef(KVS_HRL).
-define(KVS_HRL, true).

-define(USR_FEED, users).
-define(PRD_FEED, products).
-define(GRP_FEED, groups).
-define(ENT_FEED, entries).
-define(CMT_FEED, comments).
-define(FEED(Type), case Type of user -> ?USR_FEED; product -> ?PRD_FEED; group -> ?GRP_FEED; entry-> ?ENT_FEED; comment-> ?CMT_FEED;_-> undefined end).

-record(id_seq, {thing, id}).

-define(CONTAINER, id, top, entries_count=0).
-define(ITERATOR(Container, Guard), id, container=Container, feed_id, prev, next, feeds=[], guard=Guard).
-define(ITERATOR(Container), ?ITERATOR(Container, false)).
-define(CONTAINERS, [
    {feed,              record_info(fields, feed)},
    {acl,               record_info(fields, acl)},
    {entry_views,       record_info(fields, entry_views)},
    {user_transaction,  record_info(fields, user_transaction)},
    {user_payment,      record_info(fields, user_payment)} ]).

-record(container, {?CONTAINER}).
-record(iterator,  {?ITERATOR(undefined)}).

-define(CREATE_TAB(T), store_mnesia:create_table(T, record_info(fields, T), [{storage, permanent}]) ).


-endif.
