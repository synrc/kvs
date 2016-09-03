-ifndef(GROUP_HRL).
-define(GROUP_HRL, true).

-include("kvs.hrl").

-record(group,{?ITERATOR(feed),
        name=[],
        description=[],
        scope=[],
        creator=[],
        created=[],
        owner=[],
        users_count = 0,
        entries_count = 0}).

-endif.
