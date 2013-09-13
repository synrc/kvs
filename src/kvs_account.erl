-module(kvs_account).
-include_lib("kvs/include/kvs.hrl").
-include_lib("kvs/include/accounts.hrl").
-include_lib("kvs/include/membership.hrl").
-include_lib("kvs/include/payments.hrl").
-include_lib("kvs/include/feed_state.hrl").
-compile(export_all).

init(Backend) ->
    ?CREATE_TAB(id_seq),
    ?CREATE_TAB(account),
    ?CREATE_TAB(transaction),
    ok.

transaction(Account, Currency, 0, TransactionInfo) -> ok;
transaction(Account, Currency, Amount, TransactionInfo) when Amount /= 0 ->
    {Remitter, Acceptor} = if Amount > 0 -> {system, Account}; true -> {Account, system} end,
    transaction(Remitter, Acceptor, Currency, abs(Amount), TransactionInfo).
transaction(Remitter, Acceptor, Currency, Amount, TransactionInfo) ->
    TX = #transaction{id = generate_id(), acceptor = Acceptor, remitter = Remitter,
                      amount = Amount, commit_time = now(), currency = Currency, info = TransactionInfo},
    commit_transaction(TX).

debet(Account, Currency) ->
    case kvs:get(account,{Account, Currency}) of
         {ok,#account{debet = Debet}} -> {ok, Debet};
         Error -> Error end.

credit(AccountId, Currency) ->
    case kvs:get(account,{AccountId, Currency}) of
         {ok,#account{credit = Credit}} -> {ok, Credit};
         Error -> Error end.

balance(AccountId, Currency) ->
    case kvs:get(account,{AccountId, Currency}) of
         {ok, #account{debet = Debet, credit = Credit}} -> {ok, Debet - Credit};
         Error -> Error end.

create_account(AccountId) -> [ kvs:put(#account{id={AccountId, Currency}}) || Currency <- get_currencies() ].

check_quota(User) -> check_quota(User, 0).
check_quota(User, Amount) ->
    SoftLimit = kvs:get_config("accounts/quota_limit/soft",  -20),
    {ok, Balance} = balance(User, quota),
    BalanceAfterChange = Balance - Amount,
    if  BalanceAfterChange > SoftLimit -> ok;
        true ->
            HardLimit = kvs:get(config, "accounts/quota_limit/hard",  -100),
            if  BalanceAfterChange =< HardLimit -> {error, hard_limit};
                true -> {error, soft_limit} end end.

commit_transaction(#transaction{remitter = R, acceptor = A,  currency = Currency, amount = Amount} = TX) ->
    case change_accounts(R, A, Currency, Amount) of
         ok -> %mqs:notify([kvs_account, user, R, transaction], TX),
               %mqs:notify([kvs_account, user, A, transaction], TX);
                xen;
         Error -> skip end.

check_remitter_balance(RA, Amount) -> ok.

change_accounts(Remitter, Acceptor, Currency, Amount) ->
    case {kvs:get(account,{Remitter, Currency}), kvs:get(account,{Acceptor, Currency})} of
        {{ok, RA = #account{}}, {ok, AA = #account{}}}  ->
            %% check balance for remitter according to currency and amount
            case check_remitter_balance(RA, Amount) of
                ok ->   RA1 = RA#account{credit = RA#account.credit + Amount, last_change = -Amount },
                        AA1 = AA#account{debet = AA#account.debet + Amount, last_change = Amount},
                        kvs:put([AA1, RA1]);
                {error, Reason} -> {error, {remitter_balance, Reason}} end;
        {{error, Reason}, #account{}} -> {error, {remitter_account_unavailable, Reason}};
        {#account{}, {error, Reason}} -> {error, {acceptor_account_unavailable, Reason}};
        {RE, AE} -> {error, both_accounts_unavailable} end.

get_currencies() -> [internal, currency, money, quota, points].
generate_id() -> {now(),make_ref()}.
transactions(UserId) -> tx_list(UserId, undefined, 10000).

tx_list(UserId, undefined, PageAmount) ->
    case kvs:get(user_transaction, UserId) of
        {ok, O} when O#user_transaction.top =/= undefined -> tx_list(UserId, O#user_transaction.top, PageAmount);
        {error, _} -> [] end;
tx_list(UserId, StartFrom, Limit) ->
    case kvs:get(transaction,StartFrom) of
        {ok, #transaction{next = N}=P} -> [ P | kvs:traversal(transaction, #transaction.next, N, Limit)];
        X -> [] end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_notice(["kvs_account", "user", User, "transaction"] = Route,
    Message, #state{owner = Owner, type =Type} = State) ->
    error_logger:info_msg("queue_action(~p): add_transaction: Owner=~p, Route=~p, Message=~p", [self(), {Type, Owner}, Route, Message]),    
    MP = Message,
    add_transaction_to_user(User,MP),
    {noreply, State};

handle_notice(Route, Message, State) -> error_logger:info_msg("Unknown ACCOUNTS notice").

%%%%%%%%%%%%%%%%%%%%%%%%%

add_transaction_to_user(UserId,Purchase) ->
    {ok,Top} = case kvs:get(user_transaction, UserId) of
                     {ok,T} -> {ok,T};
                     _ -> error_logger:info_msg("user_transaction not found"),
                          Head = #user_transaction{ user = UserId, top = undefined},
                          {kvs:put(Head),Head}
                end,

    EntryId = Purchase#transaction.id, %kvs:next_id("membership_purchase",1),
    Prev = undefined,
    case Top#user_transaction.top of
        undefined -> Next = undefined;
        X -> case kvs:get(transaction, X) of
                 {ok, TopEntry} ->
                     Next = TopEntry#transaction.id,
                     EditedEntry = #transaction {
                           commit_time = TopEntry#transaction.commit_time,
                           amount = TopEntry#transaction.amount,
                           remitter = TopEntry#transaction.remitter,
                           acceptor = TopEntry#transaction.acceptor,
                           currency = TopEntry#transaction.currency,
                           info = TopEntry#transaction.info,
                           id = TopEntry#transaction.id,
                           next = TopEntry#transaction.next,
                           prev = EntryId },
                    kvs:put(EditedEntry); % update prev entry
                 {error, _} -> Next = undefined
             end
    end,

    Entry  = #transaction{id = EntryId,
                           commit_time = Purchase#transaction.commit_time,
                           amount = Purchase#transaction.amount,
                           remitter = Purchase#transaction.remitter,
                           acceptor = Purchase#transaction.acceptor,
                           currency = Purchase#transaction.currency,
                           info = Purchase#transaction.info,
                           next = Next,
                           prev = Prev},

    case kvs:put(Entry) of ok -> kvs:put(#user_transaction{ user = UserId, top = EntryId}), {ok, EntryId};
                              Error -> error_logger:info_msg("Cant write transaction"), {failure,Error} end.
