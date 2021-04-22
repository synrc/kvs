-ifndef(CURSORS_HRL).
-define(CURSORS_HRL, true).

-record(writer, { id    = [] :: term(),
                  count =  0 :: integer(),
                  cache = [] :: [] | {term(),term()} | {term(),term(),term()},
                  args  = [] :: [] | term(),
                  first = [] :: [] | tuple() } ).

-record(reader, { id    = [] :: [] | integer(),
                pos   =  0 :: integer() | atom(),
                cache = [] :: [] | {term(),term()} | {term(),term(),term()},
                args  = [] :: [] | integer() | term(),
                feed  = [] :: term(),
                seek = [] :: term(),
                count = 0 :: integer(),
                dir   =  0 :: 0 | 1 } ).

-endif.
