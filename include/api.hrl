-ifndef(API_HRL).
-define(API_HRL, true).
-define(API,[start/0,stop/0,leave/0,leave/1,destroy/0,destroy/1,
             join/0,join/1,modules/0,cursors/0,get/2,get/3,put/1,put/2,index/3,index/4,
             match/1,match/2,index_match/2,index_match/3,key_match/2,key_match/3,
             delete/2,delete/3,delete_range/3,
             table/1,tables/0,dir/0,initialize/2,seq/2,all/1,all/2,count/1,ver/0]).
-include("metainfo.hrl").
-spec seq(atom() | [], integer() | []) -> term().
-spec count(atom()) -> integer().
-spec dir() -> list({'table',atom()}).
-spec ver() -> {'version',string()}.
-spec leave() -> ok.
-spec destroy() -> ok.
-spec join() -> ok | {error,any()}.
-spec join(Node :: string()) -> [{atom(),any()}].
-spec modules() -> list(atom()).
-spec cursors() -> list({atom(),list(atom())}).
-spec tables() -> list(#table{}).
-spec table(Tab :: atom()) -> #table{} | false.
-endif.
