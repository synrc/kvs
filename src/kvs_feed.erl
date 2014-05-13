-module(kvs_feed).
-copyright('Synrc Research Center, s.r.o.').
-compile(export_all).
-include("entry.hrl").
-include("feed.hrl").
-include("metainfo.hrl").
-include("comment.hrl").
-include("state.hrl").

metainfo() -> 
    #schema{name=kvs,tables=[
        #table{name=entry,container=feed,fields=record_info(fields,entry),keys=[feed_id,entry_id,from]},
        #table{name=comment,container=feed,fields=record_info(fields,comment),keys=[entry_id,author_id]},
        #table{name=feed,container=true,fields=record_info(fields,feed)}
    ]}.

comments_count(entry, Eid) -> case kvs:get(entry, Eid) of {error,_} -> 0; {ok, E} -> comments_count([E],0) end;
comments_count(product, Pid)->case kvs:get(product, Pid) of {error,_}->0; {ok, P} -> comments_count([P], 0) end;
comments_count([], Acc) -> Acc;
comments_count([E|T], Acc) ->
    C = case lists:keyfind(comments, 1, element(#iterator.feeds, E)) of false -> 0;
    {_, Fid} -> case kvs:get(feed, Fid) of {error,_} -> 0;
        {ok, Feed } -> Feed#feed.entries_count 
            + comments_count(kvs:entries(Feed, comment, undefined), 0) end end,
    comments_count(T,  C + Acc).

author_comments(Who) ->
    EIDs = [E || #comment{entry_id=E} <- kvs:index(comment,from, Who) ],
    lists:flatten([ kvs:index(entry, id,EID) || EID <- EIDs]).

%% MQ API

handle_notice(  [kvs_feed,_,Owner,entry,{Eid,Fid},add],
                [#entry{feed_id=Fid}=Entry],
                #state{owner=Owner}=State) ->

                case lists:keyfind(Fid,2, State#state.feeds) of
                    false -> skip;
                    {_,_} -> add_entry({Eid,Fid},Entry) end,

                {noreply, State};

handle_notice(  [kvs_feed,_,_,comment,Cid,add],
                [#comment{id={Cid,{_,Fid}, _}}=Comment],
                #state{feeds=Feeds}=State) ->

                case lists:keyfind(Fid,2,Feeds) of
                    false -> skip;
                    {_,_}-> add_comment(Comment) end,

                {noreply, State};

handle_notice(  [kvs_feed,_,Owner,entry,{Eid,Fid},edit],
                [#entry{feed_id=Fid}=Entry],
                #state{owner=Owner, feeds=Feeds}=State) ->

                case lists:keyfind(Fid,1,Feeds) of
                    false -> skip;
                    {_,Fid}-> update_entry({Eid,Fid},Entry) end,

                {noreply, State};

handle_notice(  [kvs_feed,_,entry,delete],
                [#entry{id=Id,feed_id=Fid}=Entry],
                #state{feeds=Feeds}=State) ->

                kvs:info(?MODULE,"[kvs_feed] delete entry ~p",[Id]),
                case lists:keyfind(Fid,2,Feeds) of
                    false -> ok;
                    _ -> kvs:info(?MODULE,"[kvs_feed] => Remove entry ~p from feed ~p", [Id, Fid]),
                         kvs:remove(entry, Id),
                         msg:notify([kvs_feed, entry, Id, deleted], [Entry]) end,

                {noreply,State};

handle_notice(  [kvs_feed,Owner,delete],
                [#entry{entry_id=Eid}=Entry],
                #state{owner=Owner}=State) ->

                kvs:info(?MODULE,"[kvs_feed] delete all entries ~p ~p", [Entry#entry.entry_id, Owner]),

                [ msg:notify([kvs_feed,To,entry,delete],[Ed])
                    || #entry{to={_, To}}=Ed <- kvs:index(entry, entry_id, Eid) ],

                Fid = element(1,Entry),
                kvs:remove(entry,{Eid, Fid}),
                Removed = Entry#entry{id={Eid,Fid},feed_id=Fid},
                msg:notify([kvs_feed,entry,{Eid, Fid},deleted], [Removed]),

                {noreply, State};

handle_notice(_Route, _Message, State) -> {noreply, State}.

notify([Module|_]=Path, Message, State) ->
    case kvs:config(feeds) of
        enabled -> msg:notify(Path,Message);
        _ -> Module:handle_notice(Path,Message,State) end.

add_comment(Comment=#comment{}) ->
    kvs:info(?MODULE,"[kvs_feed] add comment: ~p to feed ~p", [Comment#comment.id, Comment#comment.feed_id]),
    C = Comment#comment{feeds=[comments]},
    Added = case kvs:add(C) of {error, E} -> {error, E}; {ok, Cm} -> Cm end,
    msg:notify([kvs_feed, comment, C#comment.id, added], [Added]).

add_entry({Eid,Fid},Entry=#entry{}) ->
    kvs:info(?MODULE,"[kvs_feed] add entry: ~p to feed ~p.", [Entry#entry.id, Entry#entry.feed_id]),
    E = Entry#entry{id = {Eid, Fid}, entry_id = Eid, feeds=[comments]},
    Added = case kvs:add(E) of {error, Err}-> {error,Err}; {ok, En} -> En end,
    msg:notify([kvs_feed, entry, E#entry.id, added], [Added]).

update_entry(Key={Eid,Fid},Entry) ->
    case kvs:get(entry,Key) of
        {error,_} -> skip;
        {ok, E} ->
            kvs:info(?MODULE,"[kvs_feed] update entry ~p in feed ~p", [Eid,Fid]),
            Updated = E#entry{description=Entry#entry.description,
                      title = Entry#entry.title,
                      media = Entry#entry.media,
                      etc   = Entry#entry.etc,
                      type  = Entry#entry.type},
            kvs:put(Updated),
            msg:notify([kvs_feed,entry,Key,updated], [Updated]) end.
