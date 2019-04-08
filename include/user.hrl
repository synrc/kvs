-ifndef(USER_HRL).
-define(USER_HRL, true).
-include("kvs.hrl").
-record(user, {?ITERATOR(feed),
        email=[]::[]|binary(),
        username=[],
        password=[],
        display_name=[],
        register_date=[],
        tokens = [],
        images=[],
        names=[],
        surnames=[],
        birth=[],
        sex=[],
        date=[],
        status=[],
        zone=[],
        type=[],
        new_field }).
-endif.
