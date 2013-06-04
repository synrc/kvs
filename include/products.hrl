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
        vendor_id              :: integer(), % auto
        categories             :: list(integer()), % admin
        ext_gift_id            :: term(),    % ext
        gift_name              :: binary(),  % admin (based on ext)
        ext_gift_name          :: binary(),  % ext
        description_short      :: binary(),  % admin (based on ext)
        description_long       :: binary(),  % admin (based on ext)
        image_small_url        :: binary(),  % admin (based on ext)
        image_big_url          :: binary(),  % admin (based on ext)
        publish_start_date     :: calendar:date_time(), % admin
        publish_end_date       :: calendar:date_time(), % admin
        real_price             :: integer(), % ext
        retailer_price         :: integer(), % ext
        our_price              :: integer(), % auto
        in_stock,            % FIXME: WTF? ext
        enabled_on_site        :: boolean(), % admin
        kakush_point           :: integer(), % auto
        kakush_currency        :: integer(), % auto
        creation_date          :: calendar:date_time(), % auto
        modify_date            :: calendar:date_time() }).

-record(product_category, {
        id             :: integer(),
        name           :: binary(),
        description    :: binary(),
        parent         :: undefined | integer() }).

