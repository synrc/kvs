-module(kvs_user).
-copyright('Synrc Research Center s.r.o.').
-include("user.hrl").
-include("group.hrl").
-include("metainfo.hrl").
-compile(export_all).

metainfo() ->
    #schema{name=kvs,tables=[
%        #table{name=user2,container=feed,fields=record_info(fields,user2)},
        #table{name=group,container=feed,fields=record_info(fields,group)},
        #table{name=user,container=feed,fields=record_info(fields,user),
                 keys=[email]}
    ]}.

