-module(feed_server_api).

handle([Module|Parameters],Message,State) ->
    Module = list_to_atom(binary_to_list(term_to_binary(Module))),
    Module:handle_notice([Module|Parameters],Message,State).
