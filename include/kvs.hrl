-ifndef(KVS_HRL).
-define(KVS_HRL, true).

-define(CUR,  id =  [] :: term(),
              top=  [] :: [] | integer(),
              bot=  [] :: [] | integer(),
              dir=   0 ::  0 | 1,
              reader=  [] :: [] | tuple(),
              writer=  [] :: [] | tuple()).
-record(cur,  {?CUR, left=0, right=0, money=0,}).

-define(ITER, id=   [] :: term(),
              next= [] :: [] | integer(),
              prev= [] :: [] | integer()).
-record(iter, {?ITER}).

-define(CONTAINER, id=[] :: [] | integer(),
                   top=[] :: [] | integer(),
                   rear=[] :: [] | integer(),
                   count=0 :: integer()).
-define(ITERATOR(Container), id=[] :: [] | integer(),
                             container=Container :: atom(),
                             feed_id=[] :: term(),
                             prev=[] :: [] | integer(),
                             next=[] :: [] | integer(),
                             feeds=[] :: list()).

-record(id_seq,    {thing, id}).
-record(container, {?CONTAINER}).
-record(iterator,  {?ITERATOR([])}).
-record(block,     {left,right,name,last}).
-record(log,       {?CONTAINER, name, acc}).
-record(operation, {?ITERATOR(log), body=[], name=[], status=[]}).
-record(kvs,       {mod,cx}).

-compile({no_auto_import,[put/2]}).

-endif.
