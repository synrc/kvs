-include("kvs.hrl").
-record(acl, {?CONTAINER}).

-record(acl_entry, {?ITERATOR(acl),
        entry_id,
        acl_id,
        accessor,
        action}).

-record(feature, {name, id, aclver}).
