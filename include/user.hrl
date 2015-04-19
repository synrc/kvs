-ifndef(USER_HRL).
-define(USER_HRL, true).

-include("kvs.hrl").

-record(user, {?ITERATOR(feed, true),
        email,
        username,
        password,
        display_name,
        register_date,
        tokens = [],
        avatar,
        names,
        surnames,
        birth,
        sex,
        date,
        status,
        zone,
        type,
        cover }).

-record(user2, {?ITERATOR(feed, true), % version 2
        everyting_getting_small,
        email,
        username,
        password,
        zone,
        type }).

-endif.
