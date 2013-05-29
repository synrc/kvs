-record(acl, {id,
        resource,
        top}).

-record(acl_entry, {id,
        entry_id,
        acl_id,
        accessor,
        action,
        next,
        prev}).

-record(feature, {name, id, aclver}).
