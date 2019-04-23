-ifndef(API_HRL).
-define(API_HRL, true).

-include("metainfo.hrl").

% exports

-export([start/0,stop/0]).                                        % service
-export([destroy/0,join/0,join/1,init/2]).                        % schema change
-export([modules/0,containers/0,tables/0,table/1,version/0]).     % meta info
-export([create/1,add/1,link/1,unlink/1,remove/2]).               % chain ops
-export([put/1,delete/2,next_id/2]).                              % raw ops
-export([get/2,get/3,index/3]).                                   % read ops
-export([load_db/1,save_db/1]).                                   % import/export


% service

-spec start() -> ok | {error,any()}.
-spec stop() -> stopped.

% schema change

-spec destroy() -> ok.
-spec join() -> ok | {error,any()}.
-spec join(Node :: string()) -> [{atom(),any()}].
-spec init(Backend :: atom(), Module :: atom()) -> list(#table{}).

% meta info

-spec modules() -> list(atom()).
-spec containers() -> list({atom(),list(atom())}).
-spec tables() -> list(#table{}).
-spec table(Tab :: atom()) -> #table{}.
-spec version() -> {version,string()}.

% chain ops

-spec create(Container :: atom()) -> integer().
-spec add(Record :: tuple()) -> {ok,tuple()} | {error,exist} | {error,no_container} | {error,just_added} | {aborted,any()}.
-spec link(Record :: tuple()) -> {ok,tuple()} | {error, not_found} | {error,no_container} | {error,just_added}.
-spec unlink(Record :: tuple()) -> {ok,tuple()} | {error,no_container}.
-spec remove(Tab :: atom(), Key :: any()) -> ok | {error,any()}.

% raw ops

-spec put(Record :: tuple()) -> ok | {error,any()}.
-spec delete(Tab :: atom(), Key :: any()) -> ok | {error,any()}.

% read ops

-spec get(Tab :: atom(), Key :: any()) -> {ok,any()} | {error,duplicated} | {error,not_found}.
-spec get(Tab :: atom(), Key :: any(), Value :: any()) -> {ok,any()} | {error,duplicated} | {error,not_found}.
-spec index(Tab :: atom(), Key :: any(), Value :: any()) -> list(tuple()).
-spec next_id(Tab :: atom() | string(), Key :: any()) -> integer().

% import/export

-spec load_db(Path :: string()) -> list(ok | {error,any()}).
-spec save_db(Path :: string()) -> ok | {error,any()}.

-endif.
