KVS: Key-Value Store Abstraction Layer
======================================

[![Actions Status](https://github.com/synrc/kvs/workflows/mix/badge.svg)](https://github.com/synrc/kvs/actions)
[![Hex pm](https://img.shields.io/hexpm/v/kvs.svg?style=flat)](https://hex.pm/packages/kvs)

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

* [2016-03-29 Вступ до KVS](https://tonpa.guru/stream/2016/2016-03-29%20KVS%20intro.txt)
* [2016-09-24 KVS STREAMS](https://tonpa.guru/stream/2016/2016-09-24%20KVS%20STREAMS.txt)
* [2018-11-13 Нова версія KVS 5.11](https://tonpa.guru/stream/2018/2018-11-13%20Новая%20версия%20KVS.htm)
* [2019-04-13 Нова версія KVS 6.4](https://tonpa.guru/stream/2019/2019-04-13%20Новая%20версия%20KVS.htm)
* [2019-07-08 KVS Ланцюжки](https://tonpa.guru/stream/2019/2019-07-08%20ERP%20BOOT.htm)
* [2026-03-01 Про доречність використання KVS](https://gist.github.com/5HT/310ed0d5f3982fb6d7572fbaa263109b)

Authors
-------

* Максим Сохацький, Інфорамційні Судові Системи
