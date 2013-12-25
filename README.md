KVS: Data Framework for KV Stores
=================================

Online Presentation: http://slid.es/maximsokhatsky/kvs

Features
--------

* Polymorphic Tuples
* Managing Linked-Lists
* Various Backends Support: KAI, Mnesia, Riak, CouchDB
* Sequential Consistency via Feed Server
* Basic Schema for Social Sites and Accounting
* Extendable Schema
* Supports Secondary Indexes for KAI, Mnesia and Riak
* Change Backends on-the-fly
* Supports Multiple backends at the same time
* Xen Ready

Overview
--------

This is database handling application that hides database access
and provides high-level rich API to stored and extend following data:

* Acl
* Users
* Groups
* Subscriptions
* Feeds
* Comments
* Meetings
* Accounts
* Payments
* Products
* Purchases

This Framework provides also a Plugin for Feed Server for sequential consistency.
All write requests with given object keys will be handled by single processes
in Feed Server so you may not worry about concurrent changes of user feeds.

All write operations that are made to data with secondary indexes,
i.e. not like linked lists could be potentially handled without feed_server.
But some KV storages are not supporting secondary indexes add those backends carefully.

Store Backends
--------------

Currently kvs includes following store backends:

* Mnesia
* Riak
* KAI

Configuring
-----------

First of all you need to tune your backend in the kvs application:

    {kvs, {dba,store_kai}},

Try to check it:

    1> kvs:config(dba).
    store_kai
    2> kvs:version().
    {version,"KVS KAI PURE XEN"}

Create database for single node:

    3> kvs:join().

Create database joining to existing cluster:

    3> kvs:join('kvs@synrc.com').

Check table packages included into the schema:

    4> kvs:dir().
    [kvs_user,kvs_product,kvs_membership,kvs_payment,kvs_feed,
     kvs_acl,kvs_account,kvs_group]

Operations
----------

Try to add some data:

    1> rr(kvs).
    2> kvs:put(#user{id="maxim@synrc.com"}).
    ok
    3> kvs:get(user,"maxim@synrc.com").
    #user{id = "maxim@synrc.com",container = feed,...}
    4> kvs:put(#user{id="doxtop@synrc.com"}).
    5> length(kvs:all(user)).
    2

Polymorphic Records
-------------------

The data in KVS represented as plain Erlang records. The first element of the tuple
as usual indicates the name of bucket. And the second element usually corresponds
to the index key field. Additional secondary indexes could be applied for stores
that supports 2i, e.g. kai, mnesia, riak.

    1 record_name -- user, groups, acl, etc... table name -- element(1, Rec).
    2 id          -- index key -- element(2, Rec).

Iterators
---------

All record could be chained into the double-linked lists in the database.
So you can inherit from the ITERATOR record just like that:

    -record(acl_entry, {?ITERATOR(acl),
        entry_id,
        acl_id,
        accessor,
        action}).

The layout of iterators are following:

    1 record_name -- table name, like
    2 id          -- index key
    3 container   -- container name
    4 feed_id     -- feed id
    5 prev        -- poniter to previous object in list
    6 next        -- next
    7 feeds       -- subfeeds
    8 guard,      -- aux field
    9 ...

This means your table will support add/remove operations to lists.

    1> kvs:add(#user{id="mes@ua.fm"}).
    2> kvs:add(#user{id="dox@ua.fm"}).
    
Read the chain (undefined means all)
    
    3> kvs:entries(kvs:get(feed, users), user, undefined).
    [#user{id="mes@ua.fm"},#user{id="dox@ua.fm"}]
    
Read flat values by all keys from table:

    4> kvs:all(user).
    [#user{id="mes@ua.fm"},#user{id="dox@ua.fm"}]

Containers
----------

If you are using iterators records this automatically means you are using containers.
Containers are just boxes for storing top/heads of the linked lists. Here is layout
of containers:

    1 record_name   -- container name
    2 id            -- unique id
    3 top           -- pointer to the list's head
    4 entries_count -- number of elements in list

Extending Schema
----------------

Usually you need only specify custom mnesia indexes and tables tuning.
Riak and KAI backends don't need it. Group you table into table packages
represented as modules with handle_notice API.

    -module(kvs_box).
    -inclue_lib("kvs/include/kvs.hrl").
    -record(box,{id,user,email}).
    -record(box_subscription,{who,whom}).
    init(Backend=store_mnesia) ->
        ?CREATE_TAB(box),
        ?CREATE_TAB(box_subscription),
        Backend:add_table_index(box, user),
        Backend:add_table_index(box, email),
        Backend:add_table_index(box_subscription, who),
        Backend:add_table_index(box_subscription, whom);
    init(_) -> ok.

And plug it into schema config:

    {kvs, {schema,[kvs_user,kvs_acl,kvs_account,...,kvs_box]}},

And on database init

    1> kvs:join().

It will create your custom schema.

Credits
-------

* Maxim Sokhatsky
* Andrii Zadorozhnii
* Vladimir Kirillov
* Alex Kalenuk
* Sergey Polkovnikov

OM A HUM
