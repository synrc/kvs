-ifndef(TEST_HRL).
-define(TEST_HRL, true).

-include("kvs.hrl").

-record(test,
{
  ?ITERATOR(feed, true),
  data
}).

-endif.
