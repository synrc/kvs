-record(membership, {
        id                         :: any(),      % package id
        no                         :: integer(),  % package number (need to display in pricelist)
        amount                     :: integer(),  % price
        currency                   :: integer(),  % currency charge
        fee                        :: integer(),  % net membership fee
        available_for_sale = false :: boolean(),  % not used yet
        quota::integer() }).

