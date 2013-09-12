-module(kvs_membership).
-author('Vladimir Baranov <baranoff.vladimir@gmail.com>').
-include_lib("kvs/include/kvs.hrl").
-include_lib("kvs/include/membership.hrl").
-include_lib("kvs/include/payments.hrl").
-include_lib("kvs/include/products.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/feed_state.hrl").
-compile(export_all).

init(Backend) ->
    ?CREATE_TAB(membership),
    ok.

add_package(#membership{}=Package)->
    Id = generate_id(),
    save_package(Package#membership{id = Id}).

list_packages(Options) ->
    Predicate = fun(MP = #membership{}) -> check_conditions(Options, MP, true) end,
    select(membership, Predicate).

list_packages()-> kvs:all(membership).

available_for_sale(PackageId, State) ->
    case kvs:get(membership,PackageId) of
        {ok, Package} -> case save_package(Package#membership{available_for_sale = State}) of
            {ok, _} -> ok;
            Error -> Error end;
        {error, Reason}-> {error, Reason} end.

generate_id()->
    Id = kvs:next_id("membership"),
    integer_to_list(Id).

add_sample_data()->
    SamplePackages = [
    #membership{no = 1, amount = 7,   currency = 0,  quota = 7,   fee = 7},
    #membership{no = 2, amount = 12,  currency = 5,  quota = 15,  fee = 7},
    #membership{no = 3, amount = 12,  currency = 0,  quota = 15,  fee = 12},
    #membership{no = 4, amount = 25,  currency = 10, quota = 30,  fee = 15},
    #membership{no = 5, amount = 30,  currency = 0,  quota = 60,  fee = 30},
    #membership{no = 6, amount = 50,  currency = 20, quota = 60,  fee = 30},
    #membership{no = 7, amount = 50,  currency = 0,  quota = 90,  fee = 50},
    #membership{no = 8, amount = 100, currency = 40, quota = 120, fee = 60}],
    WithPaymentTypes = [
        Package#membership{id = Package#membership.no} ||
            Payment <- [facebook, credit_card, wire_transfer, paypal, mobile],
            Package <- SamplePackages],
    Enabled = [P#membership{available_for_sale = true} || P <- WithPaymentTypes],
    kvs:put(Enabled).

select(RecordType, Predicate) ->
    All = kvs:all(RecordType),
    lists:filter(Predicate, All).

save_package(Package) ->
    case kvs:put([Package]) of
        ok -> {ok, Package#membership.id};
        {error, Reason}-> {error, Reason} end.

timestamp()->
    {Y, Mn, D} = erlang:date(),
    {H, M, S} = erlang:time(),
    lists:flatten(io_lib:format("~b~2..0b~2..0b_~2..0b~2..0b~2..0b", [Y, Mn, D, H, M, S])).

check_conditions(_, _, false) -> false;
check_conditions([{available_for_sale, AS}|T], MP = #membership{available_for_sale = AS1}, _) -> check_conditions(T, MP, AS == AS1);
check_conditions([{payment_type, PT}|T], MP = #membership{}, _) -> check_conditions(T, MP, true);
check_conditions([], _, true) -> true.

delete_package(PackageId) -> kvs:delete(membership, PackageId).

%% MQ API

handle_notice(["kvs_membership","system", "add_package"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("queue_action(~p): add_package: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),
    {MP} = Message,
    case add_package(MP) of
        {ok, _} -> ok;
        {error, Reason} -> error_logger:info_msg("Unable to add membership package: ~p, Reason ~p", [MP, Reason])
    end,
    {noreply, State};

handle_notice(Route, Message, State) -> error_logger:info_msg("Unknown MEMBERSHIP notice").

coalesce(undefined, B) -> B;
coalesce(A, _) -> A.
