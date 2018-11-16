KVS: Erlang Abstract Term Database
=================================
[![Build Status](https://travis-ci.org/synrc/kvs.svg?branch=master)](https://travis-ci.org/synrc/kvs)

Features
--------

* Polymorphic Tuples aka Extensible Records
* Managing Linked-Lists
* Various Backends Support: Mnesia, Riak, KAI, Redis, MongoDB
* Sequential Consistency via Feed Server
* Basic Schema for Social Sites and Accounting
* Extendable Schema
* Supports Secondary Indexes for KAI, Mnesia, Riak and MongoDB
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

MongoDB also need to add:

```erlang
{mongodb, ".*", {git, "git://github.com/comtihon/mongodb-erlang", {tag, "master"} }},
{poolboy, ".*", {git, "git://github.com/devinus/poolboy", {tag, "master"} }}
```

In the above example poolboy is optional. MongoDB and Poolboy config example:

```erlang
{kvs, [
  {mongo, [
    {connection, [{database,<<"kvs">>}]},
    {pool, [{size,10},{max_overflow,20}]}
  ]}
]}
```

To disable poolboy exclude {pool, ...} from your sys.config. More information on the 
configuring MongoDB and Poolboy can be found here: https://github.com/comtihon/mongodb-erlang, https://github.com/devinus/poolboy.

Models
------

We have built with KVS a number of applications and came up with schema samples.
We grouped schemas by three categories. KVS hides database access behind backend drivers
and provides high-level rich API to stored and extend the following data:

* **Core** — Acl, Users, Subscriptions, Feeds, Entries, Comments
* **Banking** — Account, Customer, Transaction, Item, Currency, Program, Card, Cashback
* **Social** — Group, Meeting, Payment, Product, Purchase

Applications
------------

This Framework provides also a **feed** application for sequential consistency
and **cr** application for chain replication database on top of **kvs**.
All write requests with given object key will be handled by single processes
so you may not worry about concurrent changes in user feed tops.

All write operations that are made to data with secondary indexes,
i.e. not like linked lists could be potentially handled without feed_server.
But some KV storages are not supporting secondary indexes so use these backends carefully.

Store Backends
--------------

Currently **kvs** includes following store backends:

* Mnesia
* Riak
* KAI
* Filesystem
* Redis
* MongoDB

Configuring
-----------

First of all, you need to tune your backend in the kvs application:

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

Create a database for a single node:

```erlang
3> kvs:join().
[kvs] Mnesia Init
ok
```

You can also create a database by joining to existing cluster:

```erlang
3> kvs:join('kvs@synrc.com').
```

In that case you don't need to initialize the database
to check table packages included into the schema:

```erlang
4> kvs:dir().
[{table,"id_seq"},
 {table,"subscription"},
 {table,"feed"},
 {table,"comment"},
 {table,"entry"},
 {table,"access"},
 {table,"acl"},
 {table,"user"}]
```

Operations
----------

Try to add some data:

```erlang
1> rr(kvs_user).
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

The data in KVS represented as plain Erlang records. The first element of the tuple, as usual, indicates the name of a bucket. And the second element usually corresponds
to the index key field. Additional secondary indexes could be applied for stores
that support 2i, e.g. kai, mnesia, riak, mongodb.

Iterators
---------

All record could be chained into the double-linked lists in the database.
So you can inherit from the ITERATOR record just like that:

```erlang
-record(iterator, {id,version,
                   container,feed_id,prev,
                   next,feeds=[],guard,etc}).
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

Table can be traversed from any element in any directions with ```fold/6``` processing function.

Function ```entries/3``` which read the elements from top of container can be implemented by reversing the ```fold/6``` result list:

```erlang
4> {ok,F2} = kvs:get(feed,"f").
{ok,{feed,"f",2,2,aclver}}

5> lists:reverse(kvs:fold(fun(A,Acc)->[A|Acc] end, [], entry, element(#container.top, F2),2,#iterator.prev)).
[#entry{id = 2,container = feed,feed_id = "f",prev = 1, next = [],...},
 #entry{id = 1,container = feed,feed_id = "f",prev = [], next = 2,...}]
 ```

Containers
----------

If you are using iterators records this automatically means you are using containers.
Containers are just boxes for storing top/heads of the linked lists. Here is layout
of containers:

```erlang
-record(container, {id,top,count}).
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

Usually, you need only specify custom mnesia indexes and tables tuning.
Riak, KAI and Redis backends don't need it. Group you table into table packages
represented as modules with handle_notice API.

```erlang
-module(kvs_feed).
-inclue_lib("kvs/include/metainfo.hrl").

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

Using KVS in real applications
------------------------------

Besides using KVS in production in a number of applications we have
built on top of KVS several products. The first product is Chain
Replication Database with XA protocol. And second is social Feed
Server for web shops and social sites.

### Chain Replication Database

The **kvs** semantic is totally compatible with XA protocol.
Adding the object with PUT means only putting to database
while ADD operations provide linking to the chain's container.
Also linking operation LINK is provided separately.

```erlang
dispatch({prepare,_,_,Tx}, #state{})  ->
    kvs:info(?MODULE,"KVS PUT ~p:~p~n",[element(1,Tx),element(2,Tx)]),
    kvs:put(Tx);

dispatch({commit,_,_,Tx}, #state{})  ->
    kvs:info(?MODULE,"KVS LINK ~p:~p~n",[element(1,Tx),element(2,Tx)]),
    kvs:link(Tx);

dispatch({rollback,_,_,Tx}, #state{})  ->
    kvs:info(?MODULE,"KVS REMOVE ~p:~p~n",[element(1,Tx),element(2,Tx)]),
    kvs:remove(Tx);
```

See: https://github.com/spawnproc/cr

Credits
-------

* Maxim Sokhatsky
* Andrii Zadorozhnii
* Vladimir Kirillov
* Alex Kalenuk
* Sergey Polkovnikov
* Andrey Martemyanov

OM A HUM
