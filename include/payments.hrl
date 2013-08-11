-type payment_state() :: added | done | cancelled | pending | untracked |
                         failed | unknown | confirmed | discarded.
-type payment_type():: credit_card | mobile | paypal | wire_transfer | facebook.

-record(state_change, {
        time     :: erlang:now(),
        state    :: any(),
        info     :: any()}).

-record(payment, {
        id              :: any(),
        external_id     :: any(),     % id of the purchase in external payment system if any
        user_id         :: any(),
        payment_type    :: payment_type(),
        state           :: payment_state(),
        membership,
        product,
        next            :: any(),
        prev            :: any(),
        start_time      :: erlang:now(),
        end_time        :: erlang:now(),
        state_log = []  :: [#state_change{}],
        info            :: any() }).

-record(user_payment, {
        user :: any(),
        top   :: any() }).

-define(MP_STATE_ADDED,      added).
-define(MP_STATE_DONE,       done).
-define(MP_STATE_CANCELLED,  cancelled).
-define(MP_STATE_UNTRACKED,  untracked).
-define(MP_STATE_PENDING,    pending).
-define(MP_STATE_FAILED,     failed).
-define(MP_STATE_UNKNOWN,    unknown).
-define(MP_STATE_CONFIRMED,  confirmed).
-define(MP_STATE_UNEXPECTED, unexpected).
-define(MP_STATE_DISCARDED,  discarded).
-define(MP_MONTHLY_LIMIT_MULTIPLIER, 3).

-record(pi_credit_card, {
        cardholder_name,
        cardholder_surname,
        cardnumber_masked,
        retref_num,
        prov_date,
        auth_code
        }).
