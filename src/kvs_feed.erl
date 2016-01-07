-module(kvs_feed).
-copyright('Synrc Research Center, s.r.o.').
-compile(export_all).
-include("kvs.hrl").
-include("config.hrl").
-include("entry.hrl").
-include("comment.hrl").
-include("feed.hrl").
-include("metainfo.hrl").

metainfo() ->  #schema{name=kvs,tables= core() ++ feeds() }.

core()     -> [ #table{name=config,fields=record_info(fields,config)},
                #table{name=log,container=true,fields=record_info(fields,log)},
                #table{name=operation,container=log,fields=record_info(fields,operation)},
                #table{name=id_seq,fields=record_info(fields,id_seq),keys=[thing]} ].

feeds()    -> [ #table{name=entry,container=true,fields=record_info(fields,entry)},
                #table{name=comment,container=true,fields=record_info(fields,comment)},
                #table{name=feed,container=true,fields=record_info(fields,feed)} ].
