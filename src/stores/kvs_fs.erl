-module(kvs_fs).
-include("backend.hrl").
-include("kvs.hrl").
-include("metainfo.hrl").
-include_lib("stdlib/include/qlc.hrl").
-export(?BACKEND).

db()             -> "".

start()          -> ok.

stop()           -> ok.

destroy()        -> ok.

destroy(_)       -> ok.

match(_)         -> [].

index_match(_,_) -> [].

version()        -> {version,"KVS FS"}.

leave()          -> ok.

leave(_)         -> ok.

dir()            -> [ {table,F} || F <- filelib:wildcard("data/*"), filelib:is_dir(F) ].

join(_Node,_) -> filelib:ensure_dir(dir_name()), initialize(). % should be rsync or smth

initialize() ->
    mnesia:create_schema([node()]),
    [ kvs:initialize(kvs_fs,Module) || Module <- kvs:modules() ],
    mnesia:wait_for_tables([ T#table.name || T <- kvs:tables()],infinity).

index(_Tab,_Key,_Value) -> [].
get(TableName, Key, _) ->
    HashKey = hashkey(Key),
    {ok, Dir} = dir(TableName),
    File = filename:join([Dir,HashKey]),
    case file:read_file(File) of
         {ok,Binary} -> {ok,binary_to_term(Binary,[safe])};
         {error,Reason} -> {error,Reason} end.

put(R) -> put(R,db()).
put(Records,X) when is_list(Records) -> lists:map(fun(Record) -> put(Record,X) end, Records);
put(Record,_) ->
    TableName = element(1,Record),
    HashKey = hashkey(element(2,Record)),
    BinaryValue = term_to_binary(Record),
    {ok, Dir} = dir(TableName),
    File = filename:join([Dir,HashKey]),
    filelib:ensure_dir(File),
    file:write_file(File,BinaryValue,[write,raw,binary,sync]).

dir_name() -> "data".

dir(TableName) ->
    {ok, Cwd} = file:get_cwd(),
    TablePath = filename:join([dir_name(),TableName]),
    case filelib:safe_relative_path(TablePath, Cwd) of
        unsafe -> {error, {unsafe, TablePath}};
        Target -> {ok, Target}
    end.

hashkey(Key) -> encode(base64:encode(crypto:hash(sha, term_to_binary(Key)))).

delete(TableName, Key, _) ->
    case kvs_fs:get(TableName, Key) of
        {ok,_} ->
            {ok, Dir} = dir(TableName),
            HashKey = hashkey(Key),
            File = filename:join([Dir,HashKey]),
            file:delete(File);
        {error,X} -> {error,X}
    end.
delete_range(_,_,_) -> {error, not_found}.
keys(_,_) -> [].
key_match(_,_,_) -> [].

count(RecordName) -> length(filelib:fold_files(filename:join([dir_name(), RecordName]), "",true, fun(A,Acc)-> [A|Acc] end, [])).

all(R,_) -> lists:flatten([ begin case file:read_file(File) of
                        {ok,Binary} -> binary_to_term(Binary,[safe]);
                        {error,_Reason} -> [] end end || File <-
      filelib:fold_files(filename:join([dir_name(), R]), "",true, fun(A,Acc)-> [A|Acc] end, []) ]).

seq(RecordName, Incr) -> kvs_mnesia:seq(RecordName, Incr).

create_table(Name,_Options) ->
    {ok, Dir} = dir(Name),
    file:make_dir(Dir),
    filelib:ensure_dir(Dir).

add_table_index(_Record, _Field) -> ok.

% URL ENCODE

encode(B) when is_binary(B) -> encode(binary_to_list(B));
encode([C | Cs]) when C >= $a, C =< $z -> [C | encode(Cs)];
encode([C | Cs]) when C >= $A, C =< $Z -> [C | encode(Cs)];
encode([C | Cs]) when C >= $0, C =< $9 -> [C | encode(Cs)];
encode([C | Cs]) when C == 16#20 -> [$+ | encode(Cs)];

% unreserved
encode([C = $- | Cs]) -> [C | encode(Cs)];
encode([C = $_ | Cs]) -> [C | encode(Cs)];
encode([C = 46 | Cs]) -> [C | encode(Cs)];
encode([C = $! | Cs]) -> [C | encode(Cs)];
encode([C = $~ | Cs]) -> [C | encode(Cs)];
encode([C = $* | Cs]) -> [C | encode(Cs)];
encode([C = 39 | Cs]) -> [C | encode(Cs)];
encode([C = $( | Cs]) -> [C | encode(Cs)];
encode([C = $) | Cs]) -> [C | encode(Cs)];

encode([C | Cs]) when C =< 16#7f -> escape_byte(C) ++ encode(Cs);
encode([C | Cs]) when (C >= 16#7f) and (C =< 16#07FF) ->
  escape_byte((C bsr 6) + 16#c0)
  ++ escape_byte(C band 16#3f + 16#80)
  ++ encode(Cs);
encode([C | Cs]) when (C > 16#07FF) ->
  escape_byte((C bsr 12) + 16#e0) % (0xe0 | C >> 12)
  ++ escape_byte((16#3f band (C bsr 6)) + 16#80) % 0x80 | ((C >> 6) & 0x3f)
  ++ escape_byte(C band 16#3f + 16#80) % 0x80 | (C >> 0x3f)
  ++ encode(Cs);
encode([C | Cs]) -> escape_byte(C) ++ encode(Cs);
encode([]) -> [].

hex_octet(N) when N =< 9 -> [$0 + N];
hex_octet(N) when N > 15 -> hex_octet(N bsr 4) ++ hex_octet(N band 15);
hex_octet(N) -> [N - 10 + $a].
escape_byte(C) -> normalize(hex_octet(C)).
normalize(H) when length(H) == 1 -> "%0" ++ H;
normalize(H) -> "%" ++ H.

dump() -> ok.
