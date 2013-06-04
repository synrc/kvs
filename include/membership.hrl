
-type payment_type():: credit_card | mobile | paypal | wire_transfer | facebook.

-record(membership, {
        id                         :: any(),      % package id
        payment_type               :: payment_type(),
        no                         :: integer(),  % package number (need to display in pricelist)
        amount                     :: integer(),  % price
        currency                   :: integer(),  % currency charge
        fee                        :: integer(),  % net membership fee
        available_for_sale = false :: boolean(),  % not used yet
        quota::integer() }).

