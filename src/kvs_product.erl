-module(kvs_product).
-copyright('Synrc Research Center s.r.o.').
-include_lib("kvs/include/kvs.hrl").
-include_lib("kvs/include/products.hrl").
-include_lib("kvs/include/purchases.hrl").
-include_lib("kvs/include/users.hrl").
-include_lib("kvs/include/groups.hrl").
-include_lib("kvs/include/feeds.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/config.hrl").
-include_lib("kvs/include/feed_state.hrl").
-include_lib("mqs/include/mqs.hrl").
-compile(export_all).

init(Backend) ->
    ?CREATE_TAB(product),
    ?CREATE_TAB(user_product),
    ?CREATE_TAB(product_category),
    ok.

delete(Name) ->
  case kvs:get(product, Name) of
    {ok, Product} ->
      [kvs_group:leave(Name, Gid) || Gid <- kvs_group:participate(Name)],
      kvs:remove(product, Name),
      {ok, Product};
    E -> E end.

subscription_mq(Type, Action, Who, Whom) ->
    case mqs:open([]) of
    {ok,Channel} ->
        case {Type,Action} of
        {user,add}->
            mqs_channel:bind_exchange(Channel, ?USER_EXCHANGE(Who), ?NOTIFICATIONS_EX, rk_product_feed(Whom));
        {user,remove} ->
            mqs_channel:unbind_exchange(Channel, ?USER_EXCHANGE(Who), ?NOTIFICATIONS_EX, rk_product_feed(Whom)) end,
        mqs_channel:close(Channel);
    {error,Reason} -> error_logger:info_msg("subscription_mq error: ~p",[Reason]) end.

init_mq(Product=#product{}) ->
    Groups = kvs_group:participate(Product),
    ProductExchange = ?USER_EXCHANGE(Product#product.id),
    ExchangeOptions = [{type, <<"fanout">>}, durable, {auto_delete, false}],
    case mqs:open([]) of
    {ok, Channel} ->
        error_logger:info_msg("Cration Exchange: ~p,",[{Channel,ProductExchange,ExchangeOptions}]),
        mqs_channel:create_exchange(Channel, ProductExchange, ExchangeOptions),
        Relations = build_product_relations(Product, Groups),
        [ mqs_channel:bind_exchange(Channel, ?USER_EXCHANGE(Product#product.id), ?NOTIFICATIONS_EX, Route)
            || Route <- Relations],
        mqs_channel:close(Channel);
    {error,Reason} -> error_logger:info_msg("init_mq error: ~p",[Reason]) end.

build_product_relations(Product, Groups) -> [
    mqs:key( [kvs_product, '*', Product]),
    mqs:key( [kvs_feed, product, Product, '*', '*', '*']),
    mqs:key( [kvs_feed, product, Product, '*'] ),
    mqs:key( [kvs_payment, product, Product, '*']),
    mqs:key( [kvs_account, product, Product, '*']),
    mqs:key( [kvs_meeting, product, Product, '*']),
    mqs:key( [kvs_purchase, product, Product, '*']) |
  [ mqs:key( [kvs_feed, group, G, '*', '*', '*']) || G <- Groups ]
    ].

rk_product_feed(Product) -> mqs:key([kvs_feed, product, Product, '*', '*', '*']).

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

handle_notice([kvs_product, Owner, add],
              [#product{}=Product, Recipients],
              #state{owner=Owner, feeds=Feeds}=State) ->
    error_logger:info_msg("[kvs_product] Create product ~p", [Owner]),
    Created = case kvs:add(Product) of {error, E} -> {error, E};
    {ok, #product{id=Id} = P} ->
        Params = [{id, Id}, {type, product}, {feeds, element(#iterator.feeds, P)}],
        case workers_sup:start_child(Params) of {error, E} -> {error, E};
        _ -> Entry = to_entry(P),

            [msg:notify([kvs_group, join, Gid], [{products, Id}, Entry]) || {Type, Gid} <- Recipients, Type==group],

            case lists:keyfind(products, 1, Feeds) of false -> skip;
            {_,Fid} -> msg:notify([kvs_feed, user, Owner, entry, Id, add], [Entry#entry{feed_id=Fid, to={user, Owner}}]) end,
            P
        end end,

    msg:notify([kvs_product, product, Product#product.id, added], [Created]),
    {noreply, State};

handle_notice([kvs_product, Owner, update],
              [#product{}=Product, Recipients], #state{owner=Owner} = State) ->
    error_logger:info_msg("[kvs_product] Update product ~p", [Owner]),
    case kvs:get(product, Owner) of {error,E}->
        msg:notify([kvs_product, product, Owner, updated], [{error,E}]);
    {ok, #product{}=P} ->
        Id = P#product.id,
        UpdProduct = P#product{
            title = Product#product.title,
            brief = Product#product.brief,
            cover = Product#product.cover,
            price = Product#product.price,
            currency = Product#product.currency},
        kvs:put(UpdProduct),

        Entry = to_entry(UpdProduct),

        Groups = ordsets:from_list([Gid || {Type,Gid} <- Recipients, Type==group]),
        Participate = ordsets:from_list([Gid || #group_subscription{where=Gid} <- kvs_group:participate(Id)]),
        Intersection = ordsets:intersection(Groups, Participate),
        Leave = ordsets:subtract(Participate, Intersection),
        Join = ordsets:subtract(Groups, Intersection),

        [msg:notify([kvs_group, leave, Gid], [{products, Id}]) || Gid <- Leave],
        [msg:notify([kvs_group, join, Gid], [{products, Id}, Entry]) || Gid <- Join],

        [msg:notify([kvs_feed, RouteType, To, entry, {Id, products}, edit], [Entry])
            || {RouteType, To} <- [{user, P#product.owner} | [{group, G} || G <- Intersection]]],
        msg:notify([kvs_product, product, Owner, updated], [UpdProduct])
    end,
    {noreply, State};

handle_notice([kvs_product, Owner, delete],
              [#product{}=P],
              #state{owner=Owner, feeds=Feeds}=State) ->
    error_logger:info_msg("[kvs_product] Delete product ~p ~p", [P#product.id, Owner]),
    Removed = case kvs:remove(P) of {error,E} -> {error,E};
    ok ->
        [msg:notify([kvs_group, leave, Gid], [{products, P#product.id}])
            || #group_subscription{where=Gid} <- kvs_group:participate(P#product.id)],

        case lists:keyfind(products, 1, Feeds) of false -> skip;
        {_,Fid} -> case kvs:get(entry, {P#product.id, Fid}) of {error,_} -> ok;
            {ok, E} -> msg:notify([kvs_feed, Owner, delete], [E]) end end,

        supervisor:terminate_child(workers_sup, {product, P#product.id}),
        supervisor:delete_child(workers_sup, {product, P#product.id}),
        P
    end,
    msg:notify([kvs_product, product, P#product.id, deleted], [Removed]),
    {noreply, State};

handle_notice(_Route, _Message, State) -> {noreply, State}.

to_entry(#product{}=P) ->
    Media = case P#product.cover of undefined -> #media{};
        File ->
            Thumbnail = filename:join([ filename:dirname(File), "thumbnail", filename:basename(File)]),
            [#media{url=File, thumbnail_url = Thumbnail}] end,

    #entry{ entry_id = P#product.id,
            created = P#product.created,
            from = P#product.owner,
            type = product,
            media = Media,
            title = P#product.title,
            description = P#product.brief,
            shared = ""}.
