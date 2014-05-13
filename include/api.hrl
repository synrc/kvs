-include("metainfo.hrl").

% service

-spec start() -> ok | {error,any()}.
-spec stop() -> stopped.

% schema change

-spec destroy() -> ok.
-spec join() -> ok | {error,any()}.
-spec join(Node :: string()) -> [{atom(),any()}].
-spec init_db() -> list(tuple(list(), skip | ok)).
-spec init(Backend :: atom(), Module :: atom()) -> list(#table{}).

% meta info

-spec modules() -> list(atom()).
-spec containers() -> list(tuple(atom(),list(atom()))).
-spec tables() -> list(#table{}).
-spec table(Tab :: atom()) -> #table{}.
-spec version() -> {version,string()}.

% chain ops

-spec create(Container :: atom()) -> integer().
-spec add(Record :: tuple()) -> {ok,tuple()} | {error,exist} | {error,no_container}.
-spec remove(Record :: tuple()) -> ok | {error,any()}.
-spec remove(Tab :: atom(), Key :: any()) -> ok | {error,any()}.

% raw ops

-spec put(Record :: tuple()) -> ok | {error,any()}.
-spec delete(Tab :: atom(), Key :: any()) -> ok | {error,any()}.

% read ops

-spec get(Tab :: atom(), Key :: any()) -> {ok,any()} | {error,duplicated} | {error,not_found}.
-spec get(Tab :: atom(), Key :: any(), Value :: any()) -> {ok,any()}.
-spec index(Tab :: atom(), Key :: any(), Value :: any()) -> list(tuple()).

% import/export

-spec load_db(Path :: string()) -> list(ok | {error,any()}).
-spec save_db(Path :: string()) -> ok | {error,any()}.
