-ifndef(USER_HRL).
-define(USER_HRL, true).
-include("kvs.hrl").
-record(user, {?ITERATOR(feed),
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
        type=[] }).
-record(person, {?ITER, % version 2
        mail=[]::[]|binary(),
        name=[]::[]|binary(),
        pass=[]::[]|binary(),
        zone=[]::[]|binary(),
        type=[]::[]|atom() }).
-endif.
