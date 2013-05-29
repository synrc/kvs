-ifndef(TYPES_HRL).
-define(TYPES_HRL, true).

-type username_type() :: string().
-type id_type() :: integer().
-type user_state() :: 'ok' | 'not_verified' | 'banned'.
-type proplist() :: [proplists:property()].


-endif.
