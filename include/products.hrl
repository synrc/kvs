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

-record(product, {
        id,
        ext_id                 :: term(),    % ext
        vendor_id              :: integer(), % auto
        categories             :: list(integer()), % admin
        creator,
        owner,
        feeds,
        title,
        brief,
        cover,
        publish_start_date     :: calendar:date_time(), % admin
        publish_end_date       :: calendar:date_time(), % admin
        currency,
        price,
        retailer_price         :: integer(), % ext
        our_price              :: integer(), % auto
        enabled_on_site        :: boolean(), % admin
        creation_date          :: calendar:date_time(), % auto
        modify_date            :: calendar:date_time() }).

-record(product_category, {
        id             :: integer(),
        name           :: binary(),
        description    :: binary(),
        parent         :: undefined | integer() }).

