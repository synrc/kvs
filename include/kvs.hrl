-ifndef(KVS_HRL).
-define(KVS_HRL, true).

-define(CONTAINER, id, top, count=0).
-define(ITERATOR(Container, Guard), id, version, container=Container, feed_id, prev, next, feeds=[], guard=Guard, etc).
-define(ITERATOR(Container), ?ITERATOR(Container, false)).

-record(id_seq,    {thing, id}).
-record(container, {?CONTAINER}).
-record(iterator,  {?ITERATOR(undefined)}).
-record(interval,  {left,right,name}).
-record(log,       {?CONTAINER, name, acc}).
-record(operation, {?ITERATOR(log), body, name, status}).
-record(kvs,       {mod,cx}).

-compile({no_auto_import,[put/2]}).

-include("config.hrl").
-include("metainfo.hrl").
-include("state.hrl").
-include("user.hrl").
-include("subscription.hrl").
-include("feed.hrl").
-include("entry.hrl").
-include("comment.hrl").
-include("group.hrl").
-include("acl.hrl").
-include_lib("stdlib/include/qlc.hrl").

-endif.
