KVS: Erlang Data Framework for KV databases
===========================================

Overview
--------

This is database handling application that hides database access
and provides high-level rich API to stored and extend following data:

* Acl
* Users
* Groups
* Subscriptions
* Feeds
* Accounts
* Meetings
* Payments
* Purchases

This Framework provides also a Plugin for Feed Server for sequential consostency.
All write requests with given object keys are handled by single processes in Feed Server
so you may not worry about concurrent changes of user feeds.

Store Backends
--------------

Currently kvs includes following store backends:

* Mnesia
* Riak, supports secondary indexes via LevelDB

Credits
-------

* Maxim Sokhatsky
* Andrii Zadorozhnii
* Alex Kalenuk
* Sergey Polkovnikov

OM A HUM
