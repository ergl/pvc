-module(pvc).

%% API exports
-export([connect/2,
         start_transaction/1,
         read/3,
         update/2,
         update/3,
         commit/2,
         close/1]).

-type node_ip() :: atom() | inet:ip_address().
-type partition_id() :: non_neg_integer().

%% FIXME(borja): Unexport once used
-type index_node() :: {partition_id(), node_ip()}.
-export_type([index_node/0]).

%% Socket connection options
-define(conn_options, [binary, {active, false}, {packet, 2}, {nodelay, true}]).

%% @doc Raw ring structure returned from antidote
%%
%%      Nodes are in erlang format, i.e. node_name@ip_address
-type raw_ring() :: list({partition_id(), node()}).

%% @doc Fixed ring structured used to route protocol requests
%%
%%      Uses a tuple-based structure to enable index accesses
%%      in constant time.
%%
-type fixed_ring() :: tuple().
-type cluster_sockets() :: orddict:orddict(node_ip(), inet:socket()).

-record(conn, {
    %% The IP we're using to talk to the server
    %% Used to create a transaction id
    self_ip :: string(),
    %% Ring implemented as tuples for contant access
    ring :: {non_neg_integer(), fixed_ring()},

    %% Opened sockets, one per node in the cluster
    sockets :: cluster_sockets()
}).

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

-spec connect(node_ip(), inet:port_number()) -> {ok, connection()}
                                                | {error, inet:posix()}.
connect(Address, Port) ->
    case gen_tcp:connect(Address, Port, ?conn_options) of
        {error, Reason} ->
            {error, Reason};
        {ok, Sock} ->
            connect_1(Address, Port,  Sock)
    end.

-spec connect_1(inet:hostname(), inet:port_number(), inet:socket()) -> {ok, connection()}
                                                                     | {error, inet:posix()}.
connect_1(ConnectedTo, Port, Socket) ->
    ok = gen_tcp:send(Socket, ppb_protocol_driver:connect()),
    case gen_tcp:recv(Socket, 0) of
        {error, Reason} ->
            {error, Reason};
        {ok, RawReply} ->
            {ok, RingSize, RawRing} = pvc_proto:decode_serv_reply(RawReply),
            UniqueNodes = unique_ring_nodes(RawRing),
            FixedRing = make_fixed_ring(RingSize, RawRing),
            AllSockets = open_remote_sockets(Socket, UniqueNodes, ConnectedTo, Port),
            {ok, #conn{self_ip=get_own_ip(Socket), ring={RingSize, FixedRing}, sockets=AllSockets}}
    end.

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
close(#conn{sockets=Sockets}) ->
    orddict:fold(fun(_Node, Socket, ok) ->
        gen_tcp:close(Socket)
    end, ok, Sockets).

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Get our own IP from a given socket
-spec get_own_ip(inet:socket()) -> binary().
get_own_ip(Socket) ->
    {ok, {SelfIP, _}} = inet:sockname(Socket),
    list_to_binary(inet:ntoa(SelfIP)).

%% @doc Get an unique list of the ring owning IP addresses
-spec unique_ring_nodes(raw_ring()) -> ordsets:ordset(node_ip()).
unique_ring_nodes(Ring) ->
    ordsets:from_list(lists:foldl(fun({_, Node}, Acc) ->
        [erlang_node_to_ip(Node) | Acc]
    end, [], Ring)).

%% @doc Get IP address from an erlang node name
-spec erlang_node_to_ip(atom()) -> node_ip().
erlang_node_to_ip(Node) ->
    [_, Ip] = binary:split(atom_to_binary(Node, latin1), <<"@">>),
    binary_to_atom(Ip, latin1).

%% @doc Convert a raw riak ring into a fixed tuple structure
-spec make_fixed_ring(non_neg_integer(), raw_ring()) -> fixed_ring().
make_fixed_ring(Size, RawRing) ->
    erlang:make_tuple(Size, ignore, index_ring(RawRing)).

%% @doc Converts a raw Antidote ring into an indexed structure
%%
%%      Adds a 1-based index to each entry, plus converts Erlang
%%      nodes to an IP address, for easier matching with connection
%%      sockets
%%
-spec index_ring(raw_ring()) -> [{non_neg_integer(), {partition_id(), node_ip()}}].
index_ring(RawRing) ->
    index_ring(RawRing, 1, []).

index_ring([], _, Acc) ->
    lists:reverse(Acc);

index_ring([{Partition, ErlangNode} | Rest], N, Acc) ->
    Converted = {N, {Partition, erlang_node_to_ip(ErlangNode)}},
    index_ring(Rest, N + 1, [Converted | Acc]).

%% @doc Given a list of nodes, open sockets to all except to the one given
-spec open_remote_sockets(
    inet:socket(),
    ordsets:ordset(node_ip()),
    node_ip(),
    inet:port_number()
) -> cluster_sockets().

open_remote_sockets(Socket, UniqueNodes, ConnectedTo, Port) ->
    open_remote_sockets_1(UniqueNodes, ConnectedTo, Port, [{ConnectedTo, Socket}]).

open_remote_sockets_1(Nodes, SelfNode, Port, Sockets) ->
    ordsets:fold(fun(Node, Acc) ->
        case Node of
            SelfNode ->
                Acc;

            OtherNode ->
                {ok, Sock} = gen_tcp:connect(OtherNode, Port, ?conn_options),
                orddict:store(OtherNode, Sock, Acc)
        end
    end, Sockets, Nodes).
