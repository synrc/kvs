-module(kvs_subscription).
-copyright('Synrc Research Center s.r.o.').
-include("subscription.hrl").
-include("user.hrl").
-include("metainfo.hrl").
-compile(export_all).

metainfo() ->
    #schema{name=kvs,tables=[
        #table{name=subscription,fields=record_info(fields,subscription),keys=[whom,who]},
        #table{name=id_seq,fields=record_info(fields,id_seq),keys=[thing]}
    ]}.

subscribe(Who, Whom) -> kvs:put(#subscription{key={Who,Whom},who = Who, whom = Whom}).
unsubscribe(Who, Whom) ->
    case subscribed(Who, Whom) of
        true  -> kvs:delete(subscription, {Who, Whom});
        false -> skip end.

subscriptions(#user{username = UId}) -> subscriptions(UId);
subscriptions(UId) -> kvs:index(subscription, who, UId).
subscribed(Who) -> kvs:index(subscription, whom, Who).
subscribed(Who, Whom) ->
    case kvs:get(subscription, {Who, Whom}) of
        {ok, _} -> true;
        _ -> false end.
