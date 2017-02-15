-ifndef(KVS_HRL).
-define(KVS_HRL, true).

-define(CONTAINER, id=[], top=[], rear=[], count=0).
-define(ITERATOR(Container), id=[], container=Container, feed_id=[], prev=[], next=[], feeds=[]).

-record(id_seq,    {thing, id}).
-record(container, {?CONTAINER}).
-record(iterator,  {?ITERATOR([])}).
-record(block,     {left,right,name,last}).
-record(log,       {?CONTAINER, name, acc}).
-record(operation, {?ITERATOR(log), body=[], name=[], status=[]}).
-record(kvs,       {mod,cx}).

-compile({no_auto_import,[put/2]}).

-endif.
