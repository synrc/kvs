-module(kvs_payment).
-include_lib("kvs/include/membership.hrl").
-include_lib("kvs/include/payments.hrl").
-include_lib("kvs/include/log.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/feed_state.hrl").
-compile(export_all).

user_paid(UId) ->
    {_, UP} = kvs:get(user_payment, UId),
    case UP of
        notfound -> false;
        #user_payment{top = undefined} -> false;
        _ -> true
    end.

default_if_undefined(Value, Undefined, Default) ->
    case Value of
        Undefined -> Default;
        _ -> Value end.

charge_user_account(MP) ->

    OrderId = MP#payment.id,
    Package = MP#payment.membership,
    UserId  = MP#payment.user_id,

    Currency = Package#membership.currency,
    Quota    = Package#membership.quota,

    PaymentTransactionInfo = #tx_payment{id=MP#payment.id},

    try
        kvs_account:transaction(UserId, currency, Currency, PaymentTransactionInfo),
        kvs_account:transaction(UserId, quota,    Quota,    PaymentTransactionInfo)
    catch
        _:E ->
            ?ERROR("unable to charge user account. User=~p, OrderId=~p. Error: ~p",
                   [UserId, OrderId, E])
    end.

add_payment(#payment{} = MP) -> add_payment(#payment{} = MP, undefined, undefined).
add_payment(#payment{} = MP, State0, Info) ->
    case kvs:get(payment, MP#payment.id) of
        {ok, _} -> {error, already_bought_that_one};
        {error, notfound} ->
            Start = now(),
            State = default_if_undefined(State0, undefined, ?MP_STATE_ADDED),
            StateLog = case Info of
                           undefined -> [#state_change{time = Start, state = State, info = system_change}];
                           _ -> [#state_change{time = Start, state = State, info = Info}] end,

            Id = default_if_undefined(MP#payment.id, undefined, payment_id()),
            Purchase = MP#payment{id = Id, state = State, start_time = Start, state_log = StateLog},
            %mqs:notify_purchase(Purchase),
            ?INFO("Purchase added ~p ~p",[Purchase#payment.user_id, Purchase]),
            add_to_user(Purchase#payment.user_id, Purchase)
    end.

add_to_user(UserId,Payment) ->
    {ok,Team} = case kvs:get(user_payment, UserId) of
                     {ok,T} -> ?ERROR("user_payment found"), {ok,T};
                     _ -> ?ERROR("user_payment not found"),
                          Head = #user_payment{ user = UserId, top = undefined},
                          {kvs:put(Head),Head}
                end,

    EntryId = Payment#payment.id,
    Prev = undefined,
    case Team#user_payment.top of
        undefined -> Next = undefined;
        X -> case kvs:get(payment, X) of
                {ok, TopEntry} ->
                     Next = TopEntry#payment.id,
                     EditedEntry = TopEntry#payment{next = TopEntry#payment.next, prev = EntryId},
                     kvs:put(EditedEntry);
                {error,notfound} -> Next = undefined end
    end,

    kvs:put(#user_payment{ user = UserId, top = EntryId}), % update team top with current

    Entry  = Payment#payment{id = EntryId, user_id = UserId, next = Next, prev = Prev},
    case kvs:put(Entry) of ok -> {ok, EntryId};
                           Error -> ?INFO("Cant write purchase"), {failure,Error} end.

set_payment_state(MPId, NewState, Info) ->
    case kvs:get(payment, MPId) of 
      {ok, MP} ->

    Time = now(),
    StateLog = MP#payment.state_log,
    NewStateLog = [#state_change{time = Time, state = NewState, info = Info}|StateLog],
    EndTime = case NewState of
                  ?MP_STATE_DONE -> now();
                  ?MP_STATE_CANCELLED -> now();
                  ?MP_STATE_FAILED -> now();
                  _ -> MP#payment.end_time
              end,
    Purchase = MP#payment{state = NewState, end_time = EndTime, state_log = NewStateLog},

%    mqs:notify_purchase(Purchase),
    NewMP=MP#payment{state = NewState, end_time = EndTime, state_log = NewStateLog},
    kvs:put(NewMP),

    if
        NewState == ?MP_STATE_DONE -> charge_user_account(MP); % affiliates:purchase_hook(NewMP);
        true -> ok
    end,

    ok;

    Error -> ?INFO("Can't set purchase state, not yet in db"), Error
    end.

set_payment_info(MPId, Info) ->
    {ok, MP} = kvs:get(payment, MPId),
    kvs:put(MP#payment{info = Info}).

set_payment_external_id(MPId, ExternalId) ->
    {ok, MP} = kvs:get(payment, MPId),
    case MP#payment.external_id of
        ExternalId -> ok;
        _ -> kvs:put(MP#payment{external_id = ExternalId}) end.

list_payments() -> kvs:all(payment).

list_payments(SelectOptions) ->
    Predicate = fun(MP = #payment{}) -> kvs_membership:check_conditions(SelectOptions, MP, true) end,
    kvs_membership:select(payment, Predicate).

payment_id() ->
    NextId = kvs:next_id("payment"),
    lists:concat([kvs_membership:timestamp(), "_", NextId]).

handle_notice(["kvs_payment", "user", _, "set_state"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): set_purchase_state: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),  
    {MPId, NewState, Info} = Message,
    set_payment_state(MPId, NewState, Info),
    {noreply, State};

handle_notice(["kvs_payment", "user", _, "add"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): add_purchase: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),    
    {MP} = Message,
    add_payment(MP),
    {noreply, State};

handle_notice(["kvs_payment", "user", _, "set_external_id"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): set_purchase_external_id: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {PurchaseId, TxnId} = Message,
    set_payment_external_id(PurchaseId, TxnId),
    {noreply, State};

handle_notice(["kvs_payment", "user", _, "set_info"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): set_purchase_info: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {OrderId, Info} = Message,
    set_payment_info(OrderId, Info),
    {noreply, State};

handle_notice(Route, Message, State) -> error_logger:info_msg("Unknown PAYMENTS notice").

