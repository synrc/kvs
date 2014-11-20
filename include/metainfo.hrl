-ifndef(METAINFO_HRL).
-define(METAINFO_HRL, true).

-record(schema,{name,tables=[]}).
-record(table,{name,container=feed,fields=[],keys=[],copy_type=disc_copies}).

-endif.
