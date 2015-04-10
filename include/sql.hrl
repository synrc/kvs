-ifndef(MEKAO_HRL).
-define(MEKAO_HRL, true).

-include("metainfo.hrl").

% MEKAO SQL

-record(sql_select,   {table,columns,where,order_by}).
-record(sql_insert,   {table,columns,values,returning}).
-record(sql_update,   {table,set,where,returning}).
-record(sql_delete,   {table,where,returning}).
-record(sql_settings, {placeholder,returning,is_null = fun(X) -> X == undefined end}).

-spec insert(entity(), table(), s()) -> {ok, b_query()} | {error, empty_insert}.
-spec select_pk(selector(), table(), s()) -> {ok, b_query()} | {error, pk_miss}.
-spec select(selector(), table(), s()) -> {ok, b_query()}.
-spec update_pk(selector(), table(), s()) -> {ok, b_query()} | {error, pk_miss} | {error, empty_update}.
-spec update_pk_diff( Old :: entity(), New :: entity(), table(), s()) -> {ok, b_query()} | {error, pk_miss} | {error, empty_update}.
-spec update(entity(), selector(), table(), s()) -> {ok, b_query()} | {error, empty_update}.
-spec delete_pk(selector(), table(), s()) -> {ok, b_query()} | {error, pk_miss}.
-spec delete(selector(), table(), s()) -> {ok, b_query()}.
-spec prepare_insert(entity(), table(), s()) -> p_query().
-spec prepare_select(selector(), table(), s()) -> p_query().
-spec prepare_delete(selector(), table(), s()) -> p_query().
-spec prepare_update(entity(), selector(), table(), s()) -> p_query().
-spec returning(insert | update | delete, table(), s()) -> iolist().
-spec build(p_query()) -> b_query().

-type b_query() :: 'query'(iolist()).
-type table()   :: #table{}.
-type column()  :: #column{}.
-type s()       :: #sql_settings{}.
-type entity()      :: tuple() | list().
-type selector()    :: tuple() | list(predicate(term())).
-type predicate(Value) :: Value | { '$predicate', '=' | '<>' | '>' | '>=' | '<' | '<=', Value}.
-type 'query'(Body) :: #query{body :: Body}.
-type p_query() :: 'query'( #sql_insert{}
                          | #sql_select{}
                          | #sql_update{}
                          | #sql_delete{}
                          ).

-export_type([
    table/0, column/0, s/0,
    p_query/0, b_query/0,
    predicate/1
]).

-endif.
