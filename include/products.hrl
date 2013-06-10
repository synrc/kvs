-record(ext_product_info, {
        vendor_id,
        id,
        active,
        name,
        category,
        category_name,
        short_descr,
        long_descr,
        small_image_url,
        big_image_url,
        in_stock,
        retailer_price,
        user_price }).

-record(product, {
        id                     :: integer(), % auto
        ext_id                 :: term(),    % ext
        name                   :: binary(),  % admin (based on ext)
        ext_name               :: binary(),  % ext
        vendor_id              :: integer(), % auto
        categories             :: list(integer()), % admin
        feed,
        description_short      :: binary(),  % admin (based on ext)
        description_long       :: binary(),  % admin (based on ext)
        image_small_url        :: binary(),  % admin (based on ext)
        image_big_url          :: binary(),  % admin (based on ext)
        publish_start_date     :: calendar:date_time(), % admin
        publish_end_date       :: calendar:date_time(), % admin
        price                  :: integer(), % ext
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

