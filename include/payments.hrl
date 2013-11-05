-include("kvs.hrl").

-type payment_state() :: added | done | cancelled | pending | untracked | failed | unknown | confirmed | discarded.
-type payment_type():: credit_card | mobile | paypal | wire_transfer | facebook.

-record(state_change, {
        time     :: erlang:now(),
        state    :: any(),
        info     :: any()}).

-record(user_payment,    {?CONTAINER, user}).

-record(payment, {?ITERATOR(user_payment),
        external_id     :: any(),     % id of the purchase in external payment system if any
        user_id         :: any(),
        product_id      :: any(),
        payment_type    :: payment_type(),
        state           :: payment_state(),
        membership,
        product,
        start_time      :: erlang:now(),
        end_time        :: erlang:now(),
        state_log = []  :: [#state_change{}],
        info            :: any() }).

-record(pi_credit_card, {
        cardholder_name,
        cardholder_surname,
        cardnumber_masked,
        retref_num,
        prov_date,
        auth_code
        }).
