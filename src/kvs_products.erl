-module(kvs_products).
-copyright('Synrc Research Center s.r.o.').
-include_lib("kvs/include/products.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/log.hrl").
-include_lib("kvs/include/config.hrl").
-include_lib("kvs/include/feed_state.hrl").
-include_lib("mqs/include/mqs.hrl").
-compile(export_all).

register(#product{} = Registration) ->
    Id = kvs:next_id("product", 1),
    Product = Registration#product{id = Id, feed = kvs_feed:create()},
    error_logger:info_msg("PUT PRODUCT ~p", [Product#product.id]),
    kvs:put(Product),
%    init_mq(Product),
%    mqs:notify([product, init], {E, Product#product.feed}),
    {ok, Product}.

delete(Name) ->
    case kvs:get(product, Name) of
        {ok, Product} ->
            GIds = kvs_group:participate(Name),
            [ mqs:notify(["subscription", "product", Name, "remove_from_user"], {GId}) || GId <- GIds ],
            F2U = [ {MeId, FrId} || #subscription{who = MeId, whom = FrId} <- subscriptions(Product) ],
            [ unsubscribe(MeId, FrId) || {MeId, FrId} <- F2U ],
            [ unsubscribe(FrId, MeId) || {MeId, FrId} <- F2U ],
            kvs:delete(user_status, Name),
            kvs:delete(user, Name),
            {ok, Product};
        E -> E end.

subscribe(Who, Whom) ->
    Record = #subscription{key={Who,Whom}, who = Who, whom = Whom},
    kvs:put(Record).

unsubscribe(Who, Whom) ->
    case subscribed(Who, Whom) of
        true  -> kvs:delete(subscription, {Who, Whom});
        false -> skip end.

subscriptions(undefined)-> [];
subscriptions(#product{name = UId}) -> subscriptions(UId);

subscriptions(UId) -> DBA=?DBA, DBA:subscriptions(UId).
subscribed(Who) -> DBA=?DBA, DBA:subscribed(Who).

subscribed(Who, Whom) ->
    case kvs:get(subscription, {Who, Whom}) of
        {ok, _} -> true;
        _ -> false end.

subscription_mq(Type, Action, Who, Whom) ->
    case mqs:open([]) of
        {ok,Channel} ->
            case {Type,Action} of 
                {user,add}     -> mqs_channel:bind_exchange(Channel, ?USER_EXCHANGE(Who), ?NOTIFICATIONS_EX, rk_user_feed(Whom));
                {user,remove}  -> mqs_channel:unbind_exchange(Channel, ?USER_EXCHANGE(Who), ?NOTIFICATIONS_EX, rk_user_feed(Whom)) end,
            mqs_channel:close(Channel);
        {error,Reason} -> ?ERROR("subscription_mq error: ~p",[Reason]) end.

init_mq(Product=#product{}) ->
    Groups = kvs_group:participate(Product),
    ProductExchange = ?USER_EXCHANGE(Product#product.name),
    ExchangeOptions = [{type, <<"fanout">>}, durable, {auto_delete, false}],
    case mqs:open([]) of
        {ok, Channel} ->
            ?INFO("Cration Exchange: ~p,",[{Channel,ProductExchange,ExchangeOptions}]),
            mqs_channel:create_exchange(Channel, ProductExchange, ExchangeOptions),
            Relations = build_user_relations(Product, Groups),
            [ mqs_channel:bind_exchange(Channel, ?USER_EXCHANGE(Product#product.name), ?NOTIFICATIONS_EX, Route) || Route <- Relations],
            mqs_channel:close(Channel);
        {error,Reason} -> ?ERROR("init_mq error: ~p",[Reason]) end.

build_user_relations(Product, Groups) -> [
    mqs:key( [kvs_product, '*', Product]),
    mqs:key( [kvs_feed, user, Product, '*', '*', '*']),
    mqs:key( [kvs_feed, user, Product, '*'] ),
    mqs:key( [kvs_payment, user, Product, '*']),
    mqs:key( [kvs_account, user, Product, '*']),
    mqs:key( [kvs_meeting, user, Product, '*']),
    mqs:key( [kvs_purchase, user, Product, '*']) |
  [ mqs:key( [kvs_feed, group, G, '*', '*', '*']) || G <- Groups ]
    ].

rk_user_feed(Product) -> mqs:key([kvs_feed, user, Product, '*', '*', '*']).

retrieve_connections(Id,Type) ->
    Friends = case Type of 
        user -> kvs_product:list_subscr_usernames(Id);
        _ -> kvs_group:list_group_members(Id) end,
    case Friends of
        [] -> [];
        Full -> Sub = lists:sublist(Full, 10),
            case Sub of
                [] -> [];
                _ ->
                    Data = [begin
                        case kvs:get(user,Who) of
                            {ok,Product} -> RealName = kvs_product:user_realname_user(Product),
                            Paid = kvs_payment:user_paid(Who),
                            {Who,Paid,RealName};
                        _ -> undefined end end || Who <- Sub],
                    [ X || X <- Data, X/=undefined ] end end.

handle_notice(["kvs_product", "subscribe", Who],
    Message, #state{owner = _Owner, type =_Type} = State) ->
    {Whom} = Message,
    kvs_product:subscribe(Who, Whom),
    subscription_mq(user, add, Who, Whom),
    {noreply, State};

handle_notice(["kvs_product", "unsubscribe", Who],
    Message, #state{owner = _Owner, type =_Type} = State) ->
    {Whom} = Message,
    kvs_product:unsubscribe(Who, Whom),
    subscription_mq(user, remove, Who, Whom),
    {noreply, State};

handle_notice(["kvs_product", "update", _Who],
    Message, #state{owner = _Owner, type =_Type} = State) ->
    {NewProduct} = Message,
    kvs:put(NewProduct),
    {noreply, State};

handle_notice(_Route, _Message, State) -> error_logger:info_msg("Unknown USERS notice").
