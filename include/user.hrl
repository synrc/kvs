-ifndef(USER_HRL).
-define(USER_HRL, true).

-include("kvs.hrl").

-ifndef(USER_EXT).
-define(USER_EXT, email=[]).
-endif.

-record(user, {?ITERATOR(feed), ?USER_EXT,
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
