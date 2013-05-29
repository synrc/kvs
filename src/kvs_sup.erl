-module(kvs_sup).
-behaviour(supervisor).
-export([start_link/0, stop_riak/0]).
-export([init/1]).
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

stop_riak() ->
    application:stop(riak_kv), 
    application:stop(riak_pipe),
    application:stop(eleveldb),
    application:stop(erlang_js),
    application:stop(webmachine),
    application:stop(mochiweb),
    application:stop(bitcask).

init([]) ->

  RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

 case nsx_opt:get_env(nsp_srv,riak_srv_node,0) of
    0 ->

    error_logger:info_msg("Waiting for Riak to Start...."),
    kvs:start(),
    error_logger:info_msg("Waiting for Riak to Initialize...."),
    store_app:wait_vnodes();
    _ -> skip end,

    kvs:initialize(),

 case nsx_opt:get_env(nsp_srv,riak_srv_node,0) of
    0 ->

    kvs:init_indexes(),
    case nsx_opt:get_env(store,sync_nodes,false) of
         true -> [ error_logger:info_msg("Joined: ~p ~p~n", [N, riak_core:join(N)]) || N <- nsx_opt:get_env(store, nodes, []) -- [node()] ];
         false -> skip
    end,
    case  nsx_opt:get_env(store,pass_init_db, true) of 
         false -> kvs:init_db();
         true -> pass
    end;
    _ -> skip
          end,

    {ok, { {one_for_one, 5, 10}, []} }.

