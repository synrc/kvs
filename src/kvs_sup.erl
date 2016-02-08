-module(kvs_sup).
-behaviour(supervisor).
-export([init/1, start_link/0]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).
init([])     -> {ok, { {one_for_one, 5, 10}, []} }.
