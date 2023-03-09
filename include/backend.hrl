-ifndef(BACKEND_HRL).
-define(BACKEND_HRL, true).
-define(BACKEND, [db/0,get/3,put/1,put/2,delete/3,delete_range/3,index/3,dump/0,start/0,stop/0,destroy/0,destroy/1,keys/2,
                  join/2,leave/0,leave/1,dir/0,create_table/2,add_table_index/2,seq/2,all/2,count/1,version/0,
                  match/1,key_match/3,index_match/2]).
-compile({no_auto_import,[get/1,put/2]}).
-include("kvs.hrl").
-spec put(tuple() | list(tuple())) -> ok | {error,any()}.
-spec put(tuple() | list(tuple()), #kvs{}) -> ok | {error,any()}.
-spec get(term() | any(), any(), #kvs{}) -> {ok,any()} | {error,not_found}.
-spec delete(term(), any(), #kvs{}) -> ok | {error,not_found}.
-spec delete_range(term(), any(), #kvs{}) -> ok | {error,not_found}.
-spec dump() -> ok.
-spec start() -> ok.
-spec stop() -> ok.
-spec index(term(), any(), any()) -> list(tuple()).
-endif.
