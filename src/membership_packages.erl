-module(membership_packages).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').
-include_lib("kvs/include/membership_packages.hrl").
-include_lib("kvs/include/log.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/feed_state.hrl").
-compile(export_all).

-type package_id() :: integer().
-type list_options()::[{payment_type, payment_type()}|{available_for_sale, boolean()}].

add_package(#membership_package{}=Package)->
    Id = generate_id(),
    save_package(Package#membership_package{id = Id}).

get_package(PackageId)->
    case kvs:get(membership_package, PackageId) of
        {ok, #membership_package{} = Package}-> {ok, Package};
        {error, Reason}-> {error, Reason} end.

list_packages(Options) ->
    Predicate = fun(MP = #membership_package{}) -> check_conditions(Options, MP, true) end,
    select(membership_package, Predicate).

list_packages()-> kvs:all(membership_package).

available_for_sale(PackageId, State) ->
    case get_package(PackageId) of
        {ok, Package} -> case save_package(Package#membership_package{available_for_sale = State}) of
            {ok, _} -> ok;
            Error -> Error end;
        {error, Reason}-> {error, Reason} end.

add_purchase(#membership_purchase{} = MP) -> add_purchase(#membership_purchase{} = MP, undefined, undefined).

add_purchase(#membership_purchase{} = MP, State0, Info) ->
    case kvs:get(membership_purchase, MP#membership_purchase.id) of
        {ok, _} -> {error, already_bought_that_one};
        {error, notfound} ->
            %% fill needed fields
            Start = now(),
            State = default_if_undefined(State0, undefined, ?MP_STATE_ADDED),
            %% FIXME: uniform info field if needed
            StateLog = case Info of
                           undefined ->
                               [#state_change{time = Start, state = State,
                                              info = system_change}];
                           _ ->
                               [#state_change{time = Start, state = State,
                                              info = Info}]
                       end,

            %% TODO: add check for duplicate purches if id given by external module
            Id = default_if_undefined(MP#membership_purchase.id, undefined, purchase_id()),

            Purchase = MP#membership_purchase{id = Id,
                                              state = State,
                                              start_time = Start,
                                              state_log = StateLog},

            %% notify about purchase added
%            nsx_msg:notify_purchase(Purchase),

            ?INFO("Purchase added ~p ~p",[Purchase#membership_purchase.user_id, Purchase]),

            store_riak:add_purchase_to_user(Purchase#membership_purchase.user_id, Purchase)
    end.

get_purchase(PurchaseId)->
    case kvs:get(membership_purchase, PurchaseId) of
        {ok, #membership_purchase{} = Package}-> {ok, Package};
        {error, Reason}-> {error, Reason} end.

set_purchase_state(MPId, NewState, Info) ->
    case kvs:get(membership_purchase, MPId) of 
      {ok, MP} ->

    Time = now(),
    StateLog = MP#membership_purchase.state_log,
    %% add new state to head of state log
    NewStateLog = [#state_change{time = Time, state = NewState,
                                 info = Info}|StateLog],
    EndTime = case NewState of
                  ?MP_STATE_DONE -> now();
                  ?MP_STATE_CANCELLED -> now();
                  ?MP_STATE_FAILED -> now();
                  _ -> MP#membership_purchase.end_time
              end,
    Purchase = MP#membership_purchase{state = NewState,
                                      end_time = EndTime,
                                      state_log = NewStateLog},

    %% notify aboput state change
%    nsx_msg:notify_purchase(Purchase),
    NewMP=MP#membership_purchase{state = NewState,
                                         end_time = EndTime,
                                         state_log = NewStateLog},
    kvs:put(NewMP),

    if
        NewState == ?MP_STATE_DONE ->
            charge_user_account(MP);
%            affiliates:purchase_hook(NewMP);
        true ->
            ok
    end,
    ok;
  
    Error -> ?INFO("Can't set purchase state, not yet in db"), Error
    end.

set_purchase_info(MPId, Info) ->
    {ok, MP} = kvs:get(membership_purchase, MPId),
    kvs:put(MP#membership_purchase{info = Info}).

set_purchase_external_id(MPId, ExternalId) ->
    {ok, MP} = kvs:get(membership_purchase, MPId),
    case MP#membership_purchase.external_id of
        ExternalId -> ok;
        _ -> kvs:put(MP#membership_purchase{external_id = ExternalId}) end.

list_purchases() -> kvs:all(membership_purchase).

list_purchases(SelectOptions) ->
    Predicate = fun(MP = #membership_purchase{}) -> check_conditions(SelectOptions, MP, true) end,
    select(membership_purchase, Predicate).

purchase_id() ->
    NextId = kvs:next_id("membership_purchase"),
    lists:concat([timestamp(), "_", NextId]).

add_sample_data()->
    SamplePackages = [
    #membership_package{no = 1, amount = 7,   deducted_for_gifts = 0,  quota = 7,   net_membership = 7},
    #membership_package{no = 2, amount = 12,  deducted_for_gifts = 5,  quota = 15,  net_membership = 7},
    #membership_package{no = 3, amount = 12,  deducted_for_gifts = 0,  quota = 15,  net_membership = 12},
    #membership_package{no = 4, amount = 25,  deducted_for_gifts = 10, quota = 30,  net_membership = 15},
    #membership_package{no = 5, amount = 30,  deducted_for_gifts = 0,  quota = 60,  net_membership = 30},
    #membership_package{no = 6, amount = 50,  deducted_for_gifts = 20, quota = 60,  net_membership = 30},
    #membership_package{no = 7, amount = 50,  deducted_for_gifts = 0,  quota = 90,  net_membership = 50},
    #membership_package{no = 8, amount = 100, deducted_for_gifts = 40, quota = 120, net_membership = 60}],
    WithPaymentTypes = [
        Package#membership_package{id = generate_id(), payment_type=Payment} ||
            Payment <- [facebook, credit_card, wire_transfer, paypal, mobile],
            Package <- SamplePackages],
    Enabled = [P#membership_package{available_for_sale = true} || P <- WithPaymentTypes],
    kvs:put(Enabled).

generate_id()->
    Id = kvs:next_id("membership_package"),
    integer_to_list(Id).

default_if_undefined(Value, Undefined, Default) ->
    case Value of
        Undefined -> Default;
        _ -> Value end.

charge_user_account(MP) ->
    OrderId = MP#membership_purchase.id,
    Package = MP#membership_purchase.membership_package,
    Kakush = Package#membership_package.deducted_for_gifts,
    Quota = Package#membership_package.quota,
    UserId = MP#membership_purchase.user_id,

    PaymentTransactionInfo = #tx_payment{id=MP#membership_purchase.id},

    try
        ?INFO("charge user account. OrderId: ~p, User: ~p, Kakush:~p, Quota:~p",
              [OrderId, UserId, Kakush, Quota]),

        accounts:transaction(UserId, internal, Kakush, PaymentTransactionInfo),
        accounts:transaction(UserId, quota, Quota, PaymentTransactionInfo)
    catch
        _:E ->
            ?ERROR("unable to charge user account. User=~p, OrderId=~p. Error: ~p",
                   [UserId, OrderId, E])
    end.

select(RecordType, Predicate) ->
    All = kvs:all(RecordType),
    lists:filter(Predicate, All).

save_package(Package) ->
    case kvs:put([Package]) of
        ok -> {ok, Package#membership_package.id};
        {error, Reason}-> {error, Reason} end.

timestamp()->
    {Y, Mn, D} = erlang:date(),
    {H, M, S} = erlang:time(),
    lists:flatten(io_lib:format("~b~2..0b~2..0b_~2..0b~2..0b~2..0b", [Y, Mn, D, H, M, S])).

check_conditions(_, _, false) -> false;
check_conditions([{available_for_sale, AS}|T], MP = #membership_package{available_for_sale = AS1}, _) -> check_conditions(T, MP, AS == AS1);
check_conditions([{payment_type, PT}|T], MP = #membership_package{payment_type = PT1}, _) -> check_conditions(T, MP, PT == PT1);
check_conditions([], _, true) -> true.

delete_package(PackageId) -> kvs:delete(membership_package, PackageId).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_notice(["system", "add_package"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): add_package: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {MP} = Message,
    case membership_packages:add_package(MP) of
        {ok, _} ->
            ok;
        {error, Reason} ->
            ?ERROR("Unable to add membership package: ~p, Reason ~p", [MP, Reason])
    end,
    {noreply, State};

handle_notice(["purchase", "user", _, "set_purchase_state"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): set_purchase_state: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),  
    {MPId, NewState, Info} = Message,
    membership_packages:set_purchase_state(MPId, NewState, Info),
    {noreply, State};

handle_notice(["purchase", "user", _, "add_purchase"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): add_purchase: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),    
    {MP} = Message,
    membership_packages:add_purchase(MP),
    {noreply, State};

handle_notice(["purchase", "user", _, "set_purchase_external_id"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): set_purchase_external_id: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {PurchaseId, TxnId} = Message,
    membership_packages:set_purchase_external_id(PurchaseId, TxnId),
    {noreply, State};

handle_notice(["purchase", "user", _, "set_purchase_info"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    ?INFO("queue_action(~p): set_purchase_info: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {OrderId, Info} = Message,
    membership_packages:set_purchase_info(OrderId, Info),
    {noreply, State};

handle_notice(Route, Message, State) -> error_logger:info_msg("Unknown PAYMENTS notice").

coalesce(undefined, B) -> B;
coalesce(A, _) -> A.
