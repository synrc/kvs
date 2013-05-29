-include("types.hrl").

-record(invite_code, {
        code,
        create_date,
        issuer :: username_type() | '_', %% Dialyzer and record MatchSpec warnings http://j.mp/vZ8670
        recipient,
        next,
        prev,
        created_user :: username_type() | '_'}).

-record(invite_by_issuer, {
        user,
        top}).

-record(invitation_tree, {
        user,                 % user id
        parent,               % parent user id
        next_sibling = none,  % user id of next child of the same parent
        first_child  = none,  % link to the children list
        invite_code,          % invite code, for this user
        children = empty      % can be filled in traversal
        }).

-define(INVITATION_TREE_ROOT, {root}).
