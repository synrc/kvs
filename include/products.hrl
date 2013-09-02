-include("kvs.hrl").

-record(ext_product_info, {
        vendor_id,
        id,
        active,
        title,
        brief,
        category,
        category_name,
        in_stock,
        retailer_price,
        user_price }).

-record(product, {?ITERATOR(feed, true),
        ext_id                 :: term(),    % ext
        vendor_id              :: integer(), % auto
        categories             :: list(integer()), % admin
        creator,
        owner,
        title,
        brief,
        cover,
        publish_start_date     :: calendar:date_time(), % admin
        publish_end_date       :: calendar:date_time(), % admin
        price = 0              :: integer(),
        currency               :: integer(),  % currency charge
        retailer_price         :: integer(), % ext
        our_price              :: integer(), % auto
        fee                    :: integer(),  % net membership fee
        enabled                :: boolean(), % admin
        for_sale               :: boolean(),
        created                :: calendar:date_time(), % auto
        modify_date            :: calendar:date_time() }).

-record(product_category, {
        id             :: integer(),
        name           :: binary(),
        description    :: binary(),
        parent         :: undefined | integer() }).

