-module(pvc).

%% API exports
-export([connect/2,
         start_transaction/1,
         read/3,
         update/2,
         update/3,
         commit/2,
         close/1]).

-record(conn, {}).
-record(tx_state, {}).

-opaque connection() :: #conn{}.
-opaque transaction() :: #tx_state{}.
-type abort_reason() :: atom().

-export_type([connection/0,
              transaction/0,
              abort_reason/0]).

-define(missing, erlang:error(not_implemented)).

%%====================================================================
%% API functions
%%====================================================================

-spec connect(inet:hostname(), inet:port_number()) -> {ok, connection()}
                                                    | {error, inet:posix()}.
connect(_Address, _Port) ->
    ?missing.

-spec start_transaction(term()) -> {ok, transaction()}.
start_transaction(_Id) ->
    ?missing.

-spec read(connection(), transaction(), any()) -> {ok, any(), transaction()}
                                                | {error, abort_reason()}.
read(_Conn, _Tx, Keys) when is_list(Keys) ->
    erlang:error(not_implemented);

read(_Conn, _Tx, _Key) ->
    ?missing.

-spec update(transaction(), any(), any()) -> {ok, transaction()}.
update(_Tx, _Key, _Value) ->
    ?missing.

-spec update(transaction(), [{term(), term()}]) -> {ok, transaction()}.
update(_Tx, _Updates) ->
    ?missing.

-spec commit(connection(), transaction()) -> ok | {error, abort_reason()}.
commit(_Conn, _Tx) ->
    ?missing.

-spec close(connection()) -> ok.
close(_Conn) ->
    ?missing.

%%====================================================================
%% Internal functions
%%====================================================================
