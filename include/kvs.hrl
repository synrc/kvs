-ifndef(KVS_HRL).
-define(KVS_HRL, true).
-record(id_seq, { thing = []::term(), id =  0 :: integer() } ).
-record(it,     { id    = []::[] | integer() } ).
-record(ite,    { id    = []::[] | integer(), next  = []::[] | integer() } ).
-record(iter,   { id    = []::[] | integer(), next  = []::[] | integer(), prev  = []::[] | integer() } ).
-record(kvs,    { mod   = kvs_mnesia :: kvs_mnesia | kvs_rocks | kvs_fs,
                  st    = kvs_stream :: kvs_stream | kvs_st,
                  db    = []::list(),
                  cx    = []::term() }).
-endif.
