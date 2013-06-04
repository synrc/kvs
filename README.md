KVS: Data Framework for KV Stores
=================================

Overview
--------

This is database handling application that hides database access
and provides high-level rich API to stored and extend following data:

* Acl
* Users
* Groups
* Subscriptions
* Feeds and Comments
* Meetings
* Accounts and Payments
* Products and Purchases

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
* CouchDB

Credits
-------

* Maxim Sokhatsky
* Andrii Zadorozhnii
* Alex Kalenuk
* Sergey Polkovnikov

OM A HUM
