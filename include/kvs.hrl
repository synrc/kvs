-ifndef(KVS_HRL).
-define(KVS_HRL, true).

-record(writer,  {id    = [] :: term(), % {p2p,_,_} | {muc,_}
                  count =  0 :: integer(),
                  cache = [] :: [] | tuple(),
                  args  = [] :: term(),
                  first = [] :: [] | tuple()}).

-record(reader,  {id    = [] :: term(), % phone_id | {p2p,_,_} | {muc,_,_}
                  pos   = 0 :: [] | integer(),
                  cache = [] :: [] | integer() | {atom(),term()},
                  args  = [] :: term(),
                  feed  = [] :: term(), % {p2p,_,_} | {muc,_} -- link to writer
                  dir   =  0 :: 0 | 1}).

-define(CUR,  id =  [] :: term(),
              top=  [] :: [] | integer(),
              bot=  [] :: [] | integer(),
              dir=   0 ::  0 | 1,
              reader=  [] :: [] | tuple(),
              writer=  [] :: [] | tuple()).
-record(cur,  {?CUR, left=0, right=0, args=[]::list(tuple()|integer()), money=0, status=[]}).

-define(ITER, id   = [] :: [] | integer(),
              container=[] :: atom(),
              feed = [] :: term(),
              next = [] :: [] | integer(),
              prev = [] :: [] | integer()).

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
-record(kvs,       {mod = store_mnesia,cx}).

-compile({no_auto_import,[put/2]}).

-endif.
