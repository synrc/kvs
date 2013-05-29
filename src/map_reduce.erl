-module(map_reduce).
-include_lib("log.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

get_single_tables(Setting,UId,GameFSM,_Convert, LeftList) ->
    GetPropList = fun(Key,Setngs) -> 
                   case Setngs of
                        undefined -> undefined;
                        _Else -> proplists:get_value(Key, Setngs)
                   end end,

    Rounds = GetPropList(rounds, Setting),
    GameMode = GetPropList(game_mode, Setting),
    Speed = GetPropList(speed, Setting),
    Game = GetPropList(game, Setting),
    PaidOnly = GetPropList(paid_only, Setting),
    Lucky = false,

    MaxUsers = case GameFSM of
                   "tavla" ->
                       case GameMode of standard -> 2; paired -> 10; _ -> 10 end;
                   "okey" -> 4 end,

    Check = fun(Param,Value) -> 
                   case Param of
                        undefined -> true;
                        _Else -> Param == Value
                   end end,

    Cursor = fun(Id,FilterFree,FilterUser) ->
                qlc:cursor(qlc:q([V || {{_,_,_K},_,V={game_table,creator=C,
                                                   rounds=R, game_type=G,
                                                   users=U, game_speed=S,
                                                   game_mode=GM,
                                                   paid_only = PO,
                                                   feel_lucky = L}} <- gproc:table(props),
                           FilterFree(MaxUsers - length(U)),
                           FilterUser(C,Id),
                           Check(Game,G),
                           Check(Speed,S),
                           Check(GameMode,GM),
                           Check(Rounds,R),
                           Check(Lucky, L),
                           Check(PaidOnly, PO)])
                )
    end,
    OneAvailable   = fun(N) -> N == 1 end,
    TwoAvailable   = fun(N) -> N == 2 end,
    ThreeAvailable = fun(N) -> N == 3 end,
    MoreAvailable  = fun(N) -> N > 3 end,
    NotAvailable   = fun(N) -> N == 0 end,
    Others         = fun(IterUser,CurrentUser) -> IterUser =/= CurrentUser end,
    Own            = fun(IterUser,CurrentUser) -> IterUser == CurrentUser end,

    case LeftList of
        one_other -> qlc:next_answers(Cursor(UId, OneAvailable, Others), 10);
        one_own -> qlc:next_answers(Cursor(UId, OneAvailable, Own), 10);
        two_other -> qlc:next_answers(Cursor(UId, TwoAvailable, Others), 10);
        two_own -> qlc:next_answers(Cursor(UId, TwoAvailable, Own), 10);
        three_other -> qlc:next_answers(Cursor(UId, ThreeAvailable, Others), 10);
        three_own -> qlc:next_answers(Cursor(UId, ThreeAvailable, Own), 10);
        more_other -> qlc:next_answers(Cursor(UId, MoreAvailable, Others), 10);
        more_own -> qlc:next_answers(Cursor(UId, MoreAvailable, Own), 10);
        nomore_other -> qlc:next_answers(Cursor(UId, NotAvailable, Others), 10);
        nomore_own -> qlc:next_answers(Cursor(UId, NotAvailable, Own), 10)
    end.

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
