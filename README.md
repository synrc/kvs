KVS: Chain Database Abstraction Layer
=====================================

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

[1]. <a href="https://tonpa.guru/stream/2018/2018-11-13%20Новая%20версия%20KVS.htm">2018-11-13 Нова версія KVS 5.11</a><br>
[2]. <a href="https://tonpa.guru/stream/2019/2019-04-13%20Новая%20версия%20KVS.htm">2019-04-13 Нова версія KVS 6.4</a>

Credits
-------

* Maksym Sokhatskyi
* Ihor Horobets

