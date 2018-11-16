-ifndef(CONFIG_HRL).
-define(CONFIG_HRL, true).

-record(config, {key, value=[]}).

-define(DBA, (application:get_env(kvs,dba,store_mnesia))).
-define(MQ, (kvs:config(kvs,mq,kvs))).

-endif.
