-module(kvs_rocks).
-include("backend.hrl").
-include("kvs.hrl").
-include("metainfo.hrl").
-include_lib("stdlib/include/qlc.hrl").
-export(?BACKEND).
-export([ref/0,cut/8,next/8,prev/8,prev2/8,next2/8,format/1,bt/1]).

bt([])     -> [];
bt(X)      -> binary_to_term(X,[safe]).
start()    -> ok.
stop()     -> ok.
destroy()  -> ok.
version()  -> {version,"KVS ROCKSDB"}.
dir()      -> [].
leave() -> case ref() of [] -> skip; X -> rocksdb:close(X) end.
join(_) -> application:start(rocksdb),
           leave(), {ok, Ref} = rocksdb:open(application:get_env(kvs,rocks_name,"rocksdb"), [{create_if_missing, true}]),
           initialize(),
           application:set_env(kvs,rocks_ref,Ref).
initialize() -> [ kvs:initialize(kvs_rocks,Module) || Module <- kvs:modules() ].
ref() -> application:get_env(kvs,rocks_ref,[]).
index(_,_,_) -> [].
get(Tab, Key) ->
    Address = <<(list_to_binary(lists:concat(["/",format(Tab),"/"])))/binary,
                (term_to_binary(Key))/binary>>,
%    io:format("KVS.GET.Address: ~s~n",[Address]),
    case rocksdb:get(ref(), Address, []) of
         not_found -> {error,not_found};
         {ok,Bin} -> {ok,bt(Bin)} end.

put(Records) when is_list(Records) -> lists:map(fun(Record) -> put(Record) end, Records);
put(Record) -> 
    Address = <<(list_to_binary(lists:concat(["/",format(element(1,Record)),"/"])))/binary,
                         (term_to_binary(element(2,Record)))/binary>>,
%    io:format("KVS.PUT.Address: ~s~n",[Address]),
    rocksdb:put(ref(), Address, term_to_binary(Record), [{sync,true}]).

format(X) when is_list(X) -> X;
format(X) when is_atom(X) -> atom_to_list(X);
format(X) when is_binary(X) -> binary_to_list(X);
format(X) -> io_lib:format("~p",[X]).

delete(Feed, Id) ->
    Key    = list_to_binary(lists:concat(["/",format(Feed),"/"])),
    A      = <<Key/binary,(term_to_binary(Id))/binary>>,
    rocksdb:delete(ref(), A, []).

count(_) -> 0.
all(R) -> {ok,I} = rocksdb:iterator(ref(), []),
           Key = list_to_binary(lists:concat(["/",format(R)])),
           First = rocksdb:iterator_move(I, {seek,Key}),
           lists:reverse(next(I,Key,size(Key),First,[],[],-1,0)).

next(I,Key,S,A,X,T,N,C) -> {_,L} = next2(I,Key,S,A,X,T,N,C), L.
prev(I,Key,S,A,X,T,N,C) -> {_,L} = prev2(I,Key,S,A,X,T,N,C), L.

shd([]) -> [];
shd(X) -> hd(X).

next2(_,Key,_,_,X,T,N,C) when C == N -> {shd(lists:reverse(T)),T};
next2(I,Key,S,{ok,A,X},_,T,N,C) -> next2(I,Key,S,A,X,T,N,C);
next2(_,Key,_,{error,_},X,T,_,_) -> {shd(lists:reverse(T)),T};
next2(I,Key,S,A,X,T,N,C) when size(A) > S ->
     case binary:part(A, 0, S) of Key ->
          next2(I, Key, S, rocksdb:iterator_move(I, next), [], [bt(X)|T], N, C + 1);
          _ -> {shd(lists:reverse(T)),T} end;
next2(_,Key,_,{ok,A,_},X,T,_,_) -> {bt(X),T};
next2(_,Key,_,_,X,T,_,_) -> {shd(lists:reverse(T)),T}.

prev2(_,Key,_,_,X,T,N,C) when C == N -> {bt(X),T};
prev2(I,Key,S,{ok,A,X},_,T,N,C) -> prev2(I,Key,S,A,X,T,N,C);
prev2(_,Key,_,{error,_},X,T,_,_) -> {bt(X),T};
prev2(I,Key,S,A,X,T,N,C) when size(A) > S ->
     case binary:part(A, 0, S) of Key ->
          prev2(I, Key, S, rocksdb:iterator_move(I, prev), [], [bt(X)|T], N, C + 1);
          _ -> {shd(lists:reverse(T)),T} end;
prev2(_,Key,_,{ok,A,_},X,T,_,_) -> {bt(X),T};
prev2(_,Key,_,_,X,T,_,_) -> {bt(X),T}.

cut(_,_,_,_,_,_,N,C) when C == N -> C;
cut(I,Key,S,{ok,A,X},_,T,N,C) -> prev(I,Key,S,A,X,T,N,C);
cut(_,___,_,{error,_},_,_,_,C) -> C;
cut(I,Key,S,A,_,_,N,C) when size(A) > S ->
     case binary:part(A,0,S) of Key ->
          rocksdb:delete(ref(), A, []),
          Next = rocksdb:iterator_move(I, prev),
          cut(I,Key, S, Next, [], A, N, C + 1);
                                  _ -> C end;
cut(_,_,_,_,_,_,_,C) -> C.

seq(_,_) ->
  case os:type() of
       {win32,nt} -> {Mega,Sec,Micro} = erlang:now(), integer_to_list((Mega*1000000+Sec)*1000000+Micro);
                _ -> erlang:integer_to_list(element(2,hd(lists:reverse(erlang:system_info(os_monotonic_time_source)))))
  end.

create_table(_,_) -> [].
add_table_index(_, _) -> ok.
dump() -> ok.
