-ifndef(ACL_HRL).
-define(ACL_HRL, true).

-include("kvs.hrl").

-record(acl, {?CONTAINER}).

-record(access, {?ITERATOR(acl),
        entry_id=[],
        acl_id=[],
        accessor=[],
        action=[]}).

-endif.
