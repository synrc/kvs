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
        id,
        ext_id                 :: term(),    % ext
        name                   :: string(),  % name
        display_name           :: binary(),  % admin (based on ext)
        ext_name               :: binary(),  % ext
        vendor_id              :: integer(), % auto
        categories             :: list(integer()), % admin
        creator,
        owner,
        feed,
        title,
        brief,
        description,
        title_picture,
        publish_start_date     :: calendar:date_time(), % admin
        publish_end_date       :: calendar:date_time(), % admin
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

