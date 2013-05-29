-type currency()         :: internal | quota | game_points | money | bonus.
-type account_id()       :: {string(), currency()}. %% {username, currency}.
-type transaction_id()   :: string().
-type transaction_info() :: #tx_payment{} | #tx_admin_change{} | #tx_default_assignment{}.

-record(account, {
        id :: account_id(),
        debet       :: integer(),
        credit      :: integer(),
        last_change :: integer() }).

-record(tx_payment,{ id :: integer() }).
-record(tx_admin_change,{ reason :: binary() }).
-record(tx_default_assignment,{ }).
-record(user_transaction, {user,top}).
-record(transaction, {
        id :: transaction_id(),
        commit_time :: erlang:now(),
        amount :: integer(),    %% amount to move between accounts
        remitter :: account_id(), %% accout that gives money/points
        acceptor :: account_id(), %% account receive money/points
        currency :: currency(),   %% some of the points or money
        info :: transaction_info(),
        next,
        prev }).
