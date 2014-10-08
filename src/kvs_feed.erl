-module(kvs_feed).
-copyright('Synrc Research Center, s.r.o.').
-compile(export_all).
-include("config.hrl").
-include("feed.hrl").
-include("metainfo.hrl").
-include("state.hrl").

metainfo() -> 
    #schema{name=kvs,tables=[
        #table{name=feed,container=true,fields=record_info(fields,feed)}
    ]}.
