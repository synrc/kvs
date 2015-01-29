-module(kvs_feed).
-copyright('Synrc Research Center, s.r.o.').
-compile(export_all).
-include_lib("kvs/include/config.hrl").
-include_lib("kvs/include/entry.hrl").
-include_lib("kvs/include/comment.hrl").
-include_lib("kvs/include/feed.hrl").
-include_lib("kvs/include/metainfo.hrl").
-include_lib("kvs/include/state.hrl").

metainfo() -> 
    #schema{name=kvs,tables=[
        #table{name=entry,container=true,fields=record_info(fields,entry)},
        #table{name=comment,container=true,fields=record_info(fields,comment)},
        #table
        {
            name=feed,
            container=true,
            fields=record_info(fields,feed),
            columns =
            [
                #column{name = <<"id">>, type = varchar, key = true, ro = false},
                #column{name = <<"top">>, type = varchar},
                #column{name = <<"entries_count">>, type = int},
                #column{name = <<"aclver">>, type = varchar}
            ]
        }
    ]}.
