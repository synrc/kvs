KVS: Erlang Abstract Term Database
==================================

Online Presentation: http://slid.es/maximsokhatsky/kvs

Features
--------

* Polymorphic Tuples
* Managing Linked-Lists
* Various Backends Support: Mnesia, Riak, KAI, Redis
* Sequential Consistency via Feed Server
* Basic Schema for Social Sites and Accounting
* Extendable Schema
* Supports Secondary Indexes for KAI, Mnesia and Riak
* Change Backends on-the-fly
* Supports Multiple backends at the same time
* Xen Ready

Usage
-----

In rebar.config:

```erlang
    {kvs, ".*", {git, "git://github.com/synrc/kvs", "HEAD"}}
```

Redis also need to add:
```erlang
    {eredis, ".*", {git, "git://github.com/wooga/eredis", {tag, "v1.0.6"} }}
```

Overview
--------

This is database handling application that hides database access
and provides high-level rich API to stored and extend following data:

KVS

* Acl
* Users
* Subscriptions
* Feeds
* Entries
* Comments

KVS SOCIAL

* Groups
* Meetings
* Accounts
* Payments
* Products
* Purchases

This Framework provides also a Feed Server Plugin for sequential consistency.
All write requests with given object key will be handled by single processes
in Feed Server so you may not worry about concurrent changes of user feed tops.

All write operations that are made to data with secondary indexes,
i.e. not like linked lists could be potentially handled without feed_server.
But some KV storages are not supporting secondary indexes add those backends carefully.

Store Backends
--------------

Currently kvs includes following store backends:

* Mnesia
* Riak
* KAI
* Redis

Configuring
-----------

First of all you need to tune your backend in the kvs application:

```erlang
{kvs, [{dba,store_mnesia}]},
```

Try to check it:

```erlang
1> kvs:config(dba).
store_kai

2> kvs:version().
{version,"KVS KAI PURE XEN"}
```

Create database for single node:

```erlang
3> kvs:join().
[kvs] Mnesia Init
ok
```

You can also create database by joining to existing cluster:

```erlang
3> kvs:join('kvs@synrc.com').
```

In that case you don't need to do 'init_db'
To check table packages included into the schema:

```erlang
4> kvs:dir().
[{table,"id_seq"},
 {table,"subscription"}, <- 2i
 {table,"feed"}, <- feed
 {table,"comment"},
 {table,"entry"},
 {table,"access"},
 {table,"acl"}, <- feed
 {table,"user"}]
```

Operations
----------

Try to add some data:

```erlang
1> rr(kvs).
2> kvs:put(#user{id="maxim@synrc.com"}).
ok
3> kvs:get(user,"maxim@synrc.com").
#user{id = "maxim@synrc.com",container = feed,...}
4> kvs:put(#user{id="doxtop@synrc.com"}).
5> length(kvs:all(user)).
2
```

Polymorphic Records
-------------------

The data in KVS represented as plain Erlang records. The first element of the tuple
as usual indicates the name of bucket. And the second element usually corresponds
to the index key field. Additional secondary indexes could be applied for stores
that supports 2i, e.g. kai, mnesia, riak.

Iterators
---------

All record could be chained into the double-linked lists in the database.
So you can inherit from the ITERATOR record just like that:

```erlang
-record(container, {id,top,count}).
```

The layout of iterators are following:

```erlang
> lists:zip(lists:seq(1,length((kvs:table(operation))#table.fields)),
            (kvs:table(operation))#table.fields).

[{1,id},
 {2,version},
 {3,container},
 {4,feed_id},
 {5,prev},
 {6,next},
 {7,feeds},
 {8,guard},
 {9,etc},
 {10,body},
 {11,name},
 {12,status}]
```

This means your table will support add/remove linked list operations to lists.

```erlang
1> kvs:add(#user{id="mes@ua.fm"}).
2> kvs:add(#user{id="dox@ua.fm"}).
```

Read the chain (undefined means all)

```erlang
3> kvs:entries(kvs:get(feed, user), user, undefined).
[#user{id="mes@ua.fm"},
 #user{id="dox@ua.fm"}]
```

Read flat values by all keys from table:

```erlang
4> kvs:all(user).
[#user{id="mes@ua.fm"},
 #user{id="dox@ua.fm"}]
```

Containers
----------

If you are using iterators records this automatically means you are using containers.
Containers are just boxes for storing top/heads of the linked lists. Here is layout
of containers:

```erlang
```

```erlang
> lists:zip(lists:seq(1,length((kvs:table(feed))#table.fields)),
            (kvs:table(feed))#table.fields).
[{1,id},
 {2,top},
 {3,count},
 {4,aclver}]
```

Extending Schema
----------------

Usually you need only specify custom mnesia indexes and tables tuning.
Riak, KAI and Redis backends don't need it. Group you table into table packages
represented as modules with handle_notice API.

```erlang
-module(kvs_feed).
-inclue_lib("kvs/include/kvs.hrl").

metainfo() -> 
    #schema{name=kvs,tables=[
        #table{name=feed,container=true,fields=record_info(fields,feed)},
        #table{ name=entry,container=feed,fields=record_info(fields,entry),
                keys=[feed_id,entry_id,from]},
        #table{name=comment,container=feed,fields=record_info(fields,comment),
                keys=[entry_id,author_id]} ]}.
```

And plug it into schema config:

```erlang
{kvs, {schema,[kvs_user,kvs_acl,kvs_feed,kvs_subscription]}},
```

And on database init

```erlang
1> kvs:join().
```

It will create your custom schema.

Business Logic
--------------

Here is Consumer behavior handlers of KVS FEEDS supervised processes

```erlang
handle_notice(  [kvs_feed,user,Owner,entry,Eid,add],
                [#entry{feed_id=Fid}=Entry],
                #state{feeds=Feeds}) ->

                case lists:keyfind(Fid,2, S#state.feeds) of
                    false -> skip;
                    {_,_} -> add_entry(Eid,Fid,Entry) end,
                {noreply, S};

handle_notice(  [kvs_feed,user,Owner,entry,{Eid,FeedName},edit],
                [#entry{feed_id=Fid}=Entry],
                #state{feeds=Feeds}) ->

                case lists:keyfind(FeedName,1,Feeds) of
                    false -> skip;
                    {_,Fid}-> update_entry(Eid,Fid,Entry) end,
                {noreply, S};

handle_notice(  [kvs_feed,user,Owner,entry,Eid,edit],
                [#entry{feed_id=Fid}=Entry],
                #state{feeds=Feeds}) ->

                case lists:keyfind(Fid, 2, Feeds) of
                    false -> skip;
                    {_,_} -> update_entry(Eid,Fid,Entry) end,
                {noreply, S};
```

Here is the private implementation

```erlang
add_entry(Eid,Fid,Entry) ->
    E = Entry#entry{id = {Eid, Fid}, entry_id = Eid, feeds=[comments]},
    Added = case kvs:add(E) of {error, Err} -> {error,Err}; {ok, En} -> En end,
    msg:notify([kvs_feed, entry, {Eid, Fid}, added], [Added]).

update_entry(Eid,Fid,Entry) -> ...
```

And that is how you can call it

```erlang
msg:notify([kvs_feed, user, "maxim@synrc.com", entry, Eid, add], [#entry{}]).
```

Credits
-------

* Maxim Sokhatsky
* Andrii Zadorozhnii
* Vladimir Kirillov
* Alex Kalenuk
* Sergey Polkovnikov
* Andrey Martemyanov

OM A HUM
