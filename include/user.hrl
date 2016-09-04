-ifndef(USER_HRL).
-define(USER_HRL, true).

-include("kvs.hrl").

-ifndef(USER_EXT).
-define(USER_EXT, email=[]).
-endif.

-record(user, {?ITERATOR(feed), ?USER_EXT,
        username=[],
        password=[],
        display_name=[],
        register_date=[],
        tokens = [],
        images=[],
        names=[],
        surnames=[],
        birth=[],
        sex=[],
        date=[],
        status=[],
        zone=[],
        type=[] }).

-record(user2, {?ITERATOR(feed), % version 2
        everyting_getting_small,
        email,
        username,
        password,
        zone,
        type }).

-endif.
