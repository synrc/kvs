-include("kvs.hrl").

-record(acl, {?CONTAINER}).

-record(access, {?ITERATOR(acl),
        entry_id,
        acl_id,
        accessor,
        action}).
