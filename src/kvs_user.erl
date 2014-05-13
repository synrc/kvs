-module(kvs_user).
-copyright('Synrc Research Center s.r.o.').
-include("user.hrl").
-include("config.hrl").
-include("state.hrl").
-include("metainfo.hrl").
-compile(export_all).

metainfo() -> 
    #schema{name=kvs,tables=[
        #table{name=user,container=feed,fields=record_info(fields,user),
                 keys=[facebook_id,googleplus_id,twitter_id,github_id]}
    ]}.

handle_notice([kvs_user, user, registered], {_,_,#user{id=Who}=U}, #state{owner=Who}=State)->
    kvs:info(?MODULE,"[kvs_user] process registration: ~p", [U]),
    {noreply, State};

handle_notice([kvs_user, user, Owner, delete], [#user{}=U], #state{owner=Owner}=State) ->
    kvs:info(?MODULE,"[kvs_user] delete user: ~p", [U]),
    {noreply, State};

handle_notice(_Route, _Message, State) -> 
    kvs:info(?MODULE,"[kvs_user] unknown notice."),
    {noreply, State}.
