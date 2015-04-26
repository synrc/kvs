-module(store_fs).
-copyright('Synrc Research Center s.r.o.').
-include("config.hrl").
-include("metainfo.hrl").
-include_lib("stdlib/include/qlc.hrl").
-compile(export_all).

start()    -> ok.
stop()     -> ok.
destroy()  -> ok.
version()  -> {version,"KVS FS"}.
dir()      -> [ {table,F} || F <- filelib:wildcard("data/*"), filelib:is_dir(F) ].
join(_Node) -> filelib:ensure_dir("data/"). % should be rsync or smth
change_storage(_Table,_Type) -> ok.

initialize() ->
    kvs:info(?MODULE,"fs init.~n",[]),
    mnesia:create_schema([node()]),
    [ kvs:init(store_fs,Module) || Module <- kvs:modules() ],
    mnesia:wait_for_tables([ T#table.name || T <- kvs:tables()],infinity).

index(_Tab,_Key,_Value) -> ok.
get(TableName, Key) ->
    HashKey = encode(base64:encode(crypto:hash(sha, term_to_binary(Key)))),
    Dir = lists:concat(["data/",TableName,"/"]),
    case file:read_file(lists:concat([Dir,HashKey])) of
         {ok,Binary} -> {ok,binary_to_term(Binary,[safe])};
         {error,Reason} -> {error,Reason} end.

put(Records) when is_list(Records) -> lists:map(fun(Record) -> put(Record) end, Records);
put(Record) ->
    TableName = element(1,Record),
    HashKey = encode(base64:encode(crypto:hash(sha, term_to_binary(element(2,Record))))),
    BinaryValue = term_to_binary(Record),
    Dir = lists:concat(["data/",TableName,"/"]),
    filelib:ensure_dir(Dir),
    File = lists:concat([Dir,HashKey]),
    file:write_file(File,BinaryValue,[write,raw,binary,sync]).

delete(_Tab, _Key) -> ok.
count(RecordName) -> length(filelib:fold_files(lists:concat(["data/",RecordName]), "",true, fun(A,Acc)-> [A|Acc] end, [])).
all(R) -> lists:flatten([ begin case file:read_file(File) of
                        {ok,Binary} -> binary_to_term(Binary,[safe]);
                        {error,_Reason} -> [] end end || File <-
      filelib:fold_files(lists:concat(["data/",R]), "",true, fun(A,Acc)-> [A|Acc] end, []) ]).
next_id(RecordName, Incr) -> store_mnesia:next_id(RecordName, Incr).
create_table(Name,_Options) -> filelib:ensure_dir(lists:concat(["data/",Name,"/"])).
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
