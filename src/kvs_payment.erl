-module(kvs_payment).
-include_lib("kvs/include/kvs.hrl").
-include_lib("kvs/include/membership.hrl").
-include_lib("kvs/include/payments.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/feed_state.hrl").
-compile(export_all).

init(Backend) ->
    ?CREATE_TAB(payment),
    ?CREATE_TAB(user_payment),
    Backend:add_table_index(payment, product_id),
    ok.

payments(UserId) -> payments(UserId, undefined).
payments(UserId, PageAmount) ->
    case kvs:get(user_payment, UserId) of
        {ok, O} -> kvs:entries(O, payment, PageAmount);
        {error, _} -> [] end.
payments(UserId, StartFrom, Limit) ->
    case kvs:get(payment, StartFrom) of {error,_}-> [];
        {ok, P} ->  kvs:traversal(payment, P, Limit) end.

user_paid(UId) ->
    case kvs:get(user_payment, UId) of
        {error,_} -> false;
        {ok,#user_payment{top = undefined}} -> false;
        _ -> true end.

default_if_undefined(Value, Undefined, Default) ->
    case Value of
        Undefined -> Default;
        _ -> Value end.

charge_user_account(_MP) -> ok.
%    OrderId = MP#payment.id,
%    Package = MP#payment.membership,
%    UserId  = MP#payment.user_id,
%
%    Currency = Package#membership.currency,
%    Quota    = Package#membership.quota,
%
%    PaymentTransactionInfo = #tx_payment{id=MP#payment.id},
%
%    try
%        kvs_account:transaction(UserId, currency, Currency, PaymentTransactionInfo),
%        kvs_account:transaction(UserId, quota,    Quota,    PaymentTransactionInfo)
%    catch
%        _:E ->
%            error_logger:info_msg("unable to charge user account. User=~p, OrderId=~p. Error: ~p",
%                   [UserId, OrderId, E])
%    end.

add_payment(#payment{} = MP) -> add_payment(#payment{} = MP, undefined, undefined).
add_payment(#payment{} = MP, State0, Info) ->
    error_logger:info_msg("ADD PAYMENT"),
    Start = now(),
    State = default_if_undefined(State0, undefined, 'added'),
    StateLog = case Info of
        undefined -> [#state_change{time = Start, state = State, info = system_change}];
        _ -> [#state_change{time = Start, state = State, info = Info}] end,

    Id = default_if_undefined(MP#payment.id, undefined, payment_id()),
    kvs:add(MP#payment{id = Id, state = State, start_time = Start, state_log = StateLog, feed_id=MP#payment.user_id}).

set_payment_state(MPId, NewState, Info) ->
    case kvs:get(payment, MPId) of
    {ok, MP} ->
        Time = now(),
        StateLog = MP#payment.state_log,
        NewStateLog = [#state_change{time = Time, state = NewState, info = Info}|StateLog],
        EndTime = case NewState of
                  'done' -> now();
                  'cancelled' -> now();
                  'failed' -> now();
                  _ -> MP#payment.end_time end,

        NewMP=MP#payment{state = NewState, end_time = EndTime, state_log = NewStateLog},

        Result = kvs:put(NewMP),
        msg:notify([kvs_payment, payment, NewMP#payment.id, updated], [Result]);
    Error ->
        error_logger:info_msg("[kvs_payment]Can't set purchase state, not yet in db"),
        Error end.

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
    lists:concat([timestamp(), "_", NextId]).

handle_notice([kvs_payment, Type, Owner, set_state],
              {MPId, NewState, Info},
              #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("[kvs_payment] set_state: Owner: ~p Id: ~p State: ~p", [Owner, MPId, NewState]),
    set_payment_state(MPId, NewState, Info),
    {noreply, State};

handle_notice([kvs_payment, Type, Owner, add],
              {MP},
              #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("[kvs_payment] add_purchase ~p ~p", [Owner, MP#payment.id]),
    add_payment(MP),
    {noreply, State};

handle_notice([kvs_payment, Type, Owner, set_external_id],
              {PurchaseId, TxnId},
              #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("[kvs_payment] set_external_id: Owner: ~p, Id: ~p, Tx: ~p", [Owner, PurchaseId, TxnId]),
    set_payment_external_id(PurchaseId, TxnId),
    {noreply, State};

handle_notice([kvs_payment, Type, Owner, set_info],
              {OrderId, Info},
              #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("[kvs_payment] set_info: Owner:~p Info: ~p", [Owner, Info]),
    set_payment_info(OrderId, Info),
    {noreply, State};

handle_notice(_,_,State) -> {noreply, State}.

timestamp()->
  {Y, Mn, D} = erlang:date(),
  {H, M, S} = erlang:time(),
  lists:flatten(io_lib:format("~b~2..0b~2..0b_~2..0b~2..0b~2..0b", [Y, Mn, D, H, M, S])).
