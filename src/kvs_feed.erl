-module(kvs_feed).
-copyright('Synrc Research Center, s.r.o.').
-compile(export_all).
-include("config.hrl").
-include("entry.hrl").
-include("comment.hrl").
-include("feed.hrl").
-include("metainfo.hrl").
-include("state.hrl").

metainfo() -> 
    #schema{name=kvs,tables=[
        #table{name=entry,container=true,fields=record_info(fields,entry)},
        #table{name=comment,container=true,fields=record_info(fields,comment)},
        #table{name=feed,container=true,fields=record_info(fields,feed)}
    ]}.
