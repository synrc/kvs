-module(feed_server_api).
-export([handle/3]).

handle([Module|Parameters],Message,State) ->
%    error_logger:info_msg("[feed_server_api]handle_notice Route: ~p Message ~p",[[Module|Parameters],Message]),
    M = if is_atom(Module)->Module; true-> list_to_atom(binary_to_list(term_to_binary(Module))) end,
    M:handle_notice([M|Parameters],Message,State).
