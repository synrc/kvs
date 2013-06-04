-module(kvs_sup).
-behaviour(supervisor).
-export([start_link/0]).
-export([init/1]).
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
  RestartStrategy = one_for_one,
  MaxRestarts = 1000,
  MaxSecondsBetweenRestarts = 3600,

  SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

  Restart = permanent,
  Shutdown = 2000,
  Type = worker,

  case application:get_env(kvs, riak_srv_node) of
    undefined ->
      error_logger:info_msg("Waiting for Riak to Start...."),
      kvs:start(),
      error_logger:info_msg("Waiting for Riak to Initialize....");
    {ok, _Value} -> skip end,

  kvs:initialize(),

  case application:get_env(kvs, riak_srv_node) of
    undefined ->
      case application:get_env(kvs, sync_nodes) of
         true -> [error_logger:info_msg("Joined: ~p ~p~n", [N, riak_core:join(N)]) || N <- application:get_env(kvs, nodes) -- [node()] ];
         _ -> skip end,
      case application:get_env(kvs, pass_init_db) of true -> pass; _ -> kvs:init_db() end;
    _ -> skip end,

  {ok, { {one_for_one, 5, 10}, []} }.

