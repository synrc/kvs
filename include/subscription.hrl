-ifndef(SUBSCRIPTION_HRL).
-define(SUBSCRIPTION_HRL, true).

-record(subscription,{
        key=[],
        who=[],
        whom=[],
        what=[],
        how=[],
        date=[],
        note=[]}).

-endif.
