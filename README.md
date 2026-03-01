KVS: Database Abstraction Layer
===============================

[![Actions Status](https://github.com/synrc/kvs/workflows/mix/badge.svg)](https://github.com/synrc/kvs/actions)
[![Hex pm](http://img.shields.io/hexpm/v/kvs.svg?style=flat)](https://hex.pm/packages/kvs)

Features
--------

* Polymorphic Tuples aka Extensible Records
* Basic Schema for Storing Chains
* Backends: MNESIA, FS, ROCKSDB
* Extremely Compact: 600 LOC

Usage
-----

```
$ mix deps.get
$ iex -S mix
iex(1)> :kvs.join
:ok
```

Release Notes
-------------

[1]. <a href="https://tonpa.guru/stream/2014/2014-09-29%20Версионирование%20схем%20в%20KVS.txt">2014-09-29 Версіонування схем в KVS</a><br>
[2]. <a href="https://tonpa.guru/stream/2016/2016-03-29%20KVS%20intro.txt">2016-03-29 Вступ до KVS</a><br>
[3]. <a href="https://tonpa.guru/stream/2016/2016-09-24%20KVS%20STREAMS.txt">2016-09-24 KVS STREAMS</a><br>
[4]. <a href="https://tonpa.guru/stream/2018/2018-11-13%20Новая%20версия%20KVS.htm">2018-11-13 Нова версія KVS 5.11</a><br>
[5]. <a href="https://tonpa.guru/stream/2019/2019-04-13%20Новая%20версия%20KVS.htm">2019-04-13 Нова версія KVS 6.4</a><br>
[6]. <a href="https://tonpa.guru/stream/2019/2019-07-08%20ERP%20BOOT.htm">2019-07-08 KVS Ланцюжки</a><br>
[7]. <a href="https://gist.github.com/5HT/310ed0d5f3982fb6d7572fbaa263109b">2026-03-01 Про доречність використання KVS</a>

Credits
-------

* Maksym Sokhatskyi
* Ihor Horobets

