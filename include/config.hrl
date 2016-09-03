-ifndef(CONFIG_HRL).
-define(CONFIG_HRL, true).

-record(config, {key, value=[]}).

-define(DBA, kvs:config(dba)).
-define(MQ, (kvs:config(kvs,mq,kvs))).

-endif.
