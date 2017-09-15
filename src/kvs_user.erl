-module(kvs_user).
-copyright('Synrc Research Center s.r.o.').
-include("kvs.hrl").
-include("user.hrl").
-include("group.hrl").
-include("metainfo.hrl").
-compile(export_all).

metainfo() ->
    #schema{name=kvs,tables=[
        #table{name=person,container=feed,fields=record_info(fields,person)},
        #table{name=group,container=feed,fields=record_info(fields,group)},
        #table{name=cur,container=feed,fields=record_info(fields,cur)},
        #table{name=user,container=feed,fields=record_info(fields,user),keys=[email]}
    ]}.

