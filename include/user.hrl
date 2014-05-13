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
        type }).
