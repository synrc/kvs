-module(feed_server_api).

handle([Module|Parameters],Message,State) ->
    Module = list_to_atom(binary_to_list(term_to_binary(Module))),
    error_logger:info_msg("handle_notice Route: ~p Message ~p",[[Module|Parameters],Message]),
    Module:handle_notice([Module|Parameters],Message,State).
