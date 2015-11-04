-ifndef(METAINFO_HRL).
-define(METAINFO_HRL, true).

-record(schema, {name,tables=[]}).
-record(table,  {name,container=feed,fields=[],keys=[],copy_type=application:get_env(kvs,mnesia_media,disc_copies),columns,order_by}).
-record(column, {name,type,key=false,ro=false,transform}).
-record(query,  {body,types=[],values=[],next_ph_num = 1}).

-endif.
