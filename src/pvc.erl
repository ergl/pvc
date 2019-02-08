-module(pvc).

%% API exports
-export([connect/2,
         start_transaction/2,
         read/3,
         update/3,
         update/4,
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

-type transaction_id() :: tuple().
-type partition_ws() :: orddict:orddict(term(), term()).
-type ws() :: orddict:orddict(index_node(), partition_ws()).

%% FIXME(borja): Disparity with Antidote
%% ordsets vs sets (what's more efficient on the wire?)
%% Antidote stores _only_ partition ids, we store index nodes {partition_id, node_ip}
-type read_partitions() :: sets:set(index_node()).

-type vc() :: pvc_vclock:vc(index_node()).

-record(tx_state, {
    %% Identifier of the current transaction
    %% Must be unique among all other active transactions
    id :: transaction_id(),
    %% Marks if this transaction has been read-only so fat
    %% Read only transactions won't go through 2pc
    read_only = true :: boolean(),
    %% Write set of the current transaction, partitioned
    %% by partition. This way, we can send to partitions
    %% only the subset they need to verify
    writeset = [] :: ws(),
    %% VC representing the read versions
    vc_dep = pvc_vclock:new() :: vc(),
    %% VC representing the max threshold of version that should be read
    vc_aggr = pvc_vclock:new() :: vc(),
    %% What partitions have we read before
    read_partitions = sets:new() :: read_partitions()
}).

-opaque connection() :: #conn{}.
-opaque transaction() :: #tx_state{}.
-type socket_error() :: {error, inet:posix()}.
-type abort() :: {abort, atom()}.

-export_type([connection/0,
              transaction/0,
              abort/0,
              socket_error/0]).

-define(missing, erlang:error(not_implemented)).

%%====================================================================
%% API functions
%%====================================================================

-spec connect(node_ip(), inet:port_number()) -> {ok, connection()}
                                              | socket_error().

connect(Address, Port) ->
    case gen_tcp:connect(Address, Port, ?conn_options) of
        {error, Reason} ->
            {error, Reason};
        {ok, Sock} ->
            connect_1(Address, Port,  Sock)
    end.

-spec connect_1(inet:hostname(), inet:port_number(), inet:socket()) -> {ok, connection()}
                                                                     | socket_error().

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

%% @doc Start a new Transaction. Id should be unique for this node.
-spec start_transaction(connection(), term()) -> {ok, transaction()}.
start_transaction(#conn{self_ip=IP}, Id) ->
    {ok, #tx_state{id={IP, Id}}}.

%% @doc Read a key, or a list of keys.
%%
%%      If given a list of keys, will return a list of values,
%%      in the same order as the original keys.
%%
-spec read(connection(), transaction(), any()) -> {ok, any(), transaction()}
                                                | abort()
                                                | socket_error().

read(Conn, Tx, Keys) when is_list(Keys) ->
    catch read_batch(Conn, Keys, [], Tx);

read(Conn, Tx, Key) ->
    read_internal(Key, Conn, Tx).

%% @doc Update the given Key. Old value is replaced with new one
-spec update(connection(), transaction(), any(), any()) -> {ok, transaction()}.
update(Conn, Tx = #tx_state{writeset=WS}, Key, Value) ->
    NewWS = update_internal(Conn, Key, Value, WS),
    {ok, Tx#tx_state{read_only=false, writeset=NewWS}}.

%% @doc Update a batch of keys. Old values are replaced with the new ones.
-spec update(connection(), transaction(), [{term(), term()}]) -> {ok, transaction()}.
update(Conn, Tx = #tx_state{writeset=WS}, Updates) when is_list(Updates) ->
    NewWS = lists:foldl(fun({Key, Value}, AccWS) ->
        update_internal(Conn, Key, Value, AccWS)
    end, WS, Updates),
    {ok, Tx#tx_state{read_only=false, writeset=NewWS}}.

-spec commit(connection(), transaction()) -> ok | abort().
commit(_Conn, #tx_state{read_only=true}) ->
    ok;

commit(_Conn, _Tx) ->
    ?missing.

-spec close(connection()) -> ok.
close(#conn{sockets=Sockets}) ->
    orddict:fold(fun(_Node, Socket, ok) ->
        gen_tcp:close(Socket)
    end, ok, Sockets).

%%====================================================================
%% Connect Internal functions
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

%%====================================================================
%% Routing Internal functions
%%====================================================================

-spec get_key_indexnode(connection(), term()) -> index_node().
get_key_indexnode(#conn{ring = {Size, Layout}}, Key) ->
    Pos = convert_key(Key) rem Size + 1,
    erlang:element(Pos, Layout).

-spec convert_key(term()) -> non_neg_integer().
convert_key(Key) when is_binary(Key) ->
    try
        abs(binary_to_integer(Key))
    catch _:_ ->
        %% Looked into the internals of riak_core for this
        HashedKey = crypto:hash(sha, term_to_binary({<<"antidote">>, Key})),
        abs(crypto:bytes_to_integer(HashedKey))
    end;

convert_key(Key) when is_integer(Key) ->
    abs(Key);

convert_key(TermKey) ->
    HashedKey = crypto:hash(sha, term_to_binary({<<"antidote">>, term_to_binary(TermKey)})),
    abs(crypto:bytes_to_integer(HashedKey)).

%%====================================================================
%% Read Internal functions
%%====================================================================

%% @doc Accumulatevly read the given keys.
%%
%%      On any error, return inmediately and stop reading further keys.
%%      Will not return any read value in that case.
%%
-spec read_batch(connection(), [term()], [term()], transaction()) -> {ok, [term()], transaction()}
                                                                   | abort()
                                                                   | socket_error().

read_batch(_, [], ReadAcc, AccTx) ->
    {ok, lists:reverse(ReadAcc), AccTx};

read_batch(Conn, [Key | Rest], ReadAcc, AccTx) ->
    case read_internal(Key, Conn, AccTx) of
        {ok, Value, NewTx} ->
            read_batch(Conn, Rest, [Value | ReadAcc], NewTx);
        {abort, _}=Abort ->
            throw(Abort);
        {error, _}=Err ->
            throw(Err)
    end.

-spec read_internal(term(), connection(), transaction()) -> {ok, term(), transaction()}
                                                          | abort()
                                                          | socket_error().

read_internal(Key, Conn=#conn{sockets=Sockets}, Tx=#tx_state{writeset=WS}) ->
    case key_updated(Conn, Key, WS) of
        {ok, Value} ->
            {ok, Value, Tx};
        {false, {_, Node}=IndexNode} ->
            Socket = orddict:fetch(Node, Sockets),
            remote_read(IndexNode, Socket, Key, Tx)
    end.

-spec remote_read(index_node(), inet:socket(), term(), transaction()) -> {ok, term(), transaction()}
                                                                       | abort()
                                                                       | socket_error().

remote_read({Partition, _Node}=IndexNode, Socket, Key, Tx = #tx_state{vc_aggr=Aggr,
                                                                      read_partitions=HasRead}) ->

    ReadRequest = ppb_protocol_driver:read_request(Partition, Key, Aggr, HasRead),
    ok = gen_tcp:send(Socket, ReadRequest),
    case gen_tcp:recv(Socket, 0) of
        {error, Reason} ->
            {error, Reason};
        {ok, RawReply} ->
            case pvc_proto:decode_serv_reply(RawReply) of
                {error, Aborted} ->
                    {abort, Aborted};
                {ok, Value, VersionVC, MaxVC} ->
                    {ok, Value, update_transacion(IndexNode, VersionVC, MaxVC, Tx)}
            end
    end.

%% @doc Update a transaction after a read.
-spec update_transacion(index_node(), vc(), vc(), transaction()) -> transaction().
update_transacion(IndexNode, VersionVC, MaxVC, Tx = #tx_state{vc_dep=VCdep,
                                                              vc_aggr=VCaggr,
                                                              read_partitions=HasRead}) ->
    Tx#tx_state{vc_dep = pvc_vclock:max(VersionVC, VCdep),
                vc_aggr = pvc_vclock:max(MaxVC, VCaggr),
                read_partitions = sets:add_element(IndexNode, HasRead)}.


%%====================================================================
%% Update Internal functions
%%====================================================================

%% @doc Check if a key has an assoc value in the given transaction writeset
%%
%%      Returns {ok, Value} if it was updated, or {false, Node} if not, where
%%      Node is the remote Antidote node we should route or read request
%%
-spec key_updated(connection(), term(), ws()) -> {ok, term()} | {false, index_node()}.
key_updated(Conn, Key, WS) ->
    Node = get_key_indexnode(Conn, Key),
    NodeWS = get_node_writeset(Node, WS),
    case orddict:find(Key, NodeWS) of
        {ok, Value} ->
            {ok, Value};
        error ->
            {false, Node}
    end.

-spec get_node_writeset(node(), ws()) -> partition_ws().
get_node_writeset(Node, WS) ->
    case orddict:find(Node, WS) of
        error ->
            orddict:new();
        {ok, NodeWS} ->
            NodeWS
    end.

-spec update_internal(connection(), term(), term(), ws()) -> ws().
update_internal(Conn, Key, Value, WS) ->
    Node = get_key_indexnode(Conn, Key),
    NewNodeWS = orddict:store(Key, Value, get_node_writeset(Node, WS)),
    orddict:store(Node, NewNodeWS, WS).
