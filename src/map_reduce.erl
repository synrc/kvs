-module(map_reduce).
-author('Maxim Sokhatsky').
-include_lib("kvs/include/log.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

map_reduce(Module, Fun, Args)->
    lists:flatten([ case rpc:call(Node, Module, Fun, Args) of
                       {badrpc, _Reason} -> [];
                       R -> R end || Node <- nodes()]).

map_call(NodeType,Module,Fun,Args=[ID|Rest],NodeHash0) ->

    Node = nsx_opt:get_env(nsx_idgen,game_pool,5000000) div 1000000,

    NodeHash = case NodeHash0 of
                    string_map -> fun(X) -> string_map(X) end;
                    long_map -> fun(X) -> long_map(X) end end,

    DefaultNodeAtom = case NodeType of
                           "app" -> app_srv_node;
                           "game" -> game_srv_node;
                           "public" -> web_srv_node end,

    ServerNode = case Node of
                      4 -> nsx_opt:get_env(store,DefaultNodeAtom,'maxim@synrc.com');
                      5 -> nsx_opt:get_env(store,DefaultNodeAtom,'maxim@synrc.com');
                      _ -> list_to_atom(NodeType ++ "@srv" ++ 
                           integer_to_list(NodeHash(ID)) ++
                           ".synrc.com") end,

    ?INFO("map_call: ~p",[{ServerNode,Module,Fun,Args}]),
    rpc:call(ServerNode,Module,Fun,Args).

string_map(X) -> lists:foldl(fun(A,Sum)->A+Sum end,0,X) rem 3+1.
long_map(X)   -> X div 1000000.
 
consumer_pid(Args)       -> map_call("app", feed_server,pid,Args,string_map).
cached_feed(Args)        -> map_call("app", feed_writer,cached_feed,Args,string_map).
cached_direct(Args)      -> map_call("app", feed_writer,cached_direct,Args,string_map).
cached_friends(Args)     -> map_call("app", feed_writer,cached_friends,Args,string_map).
cached_groups(Args)      -> map_call("app", feed_writer,cached_groups,Args,string_map).
start_worker(Args)       -> map_call("app", feed_launcher,start_worker,Args,string_map).
