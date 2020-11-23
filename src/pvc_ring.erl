-module(pvc_ring).

-include("pvc.hrl").

%% API
-export([partition_info/2,
         grb_replica_info/2,
         grb_replica_info/3,
         random_indexnode/1,
         get_key_indexnode/2,
         get_key_indexnode/3]).


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
-record(ring, {
    size :: non_neg_integer(),
    fixed_ring :: fixed_ring()
}).

-opaque ring() :: #ring{}.
-export_type([ring/0]).

%% @doc Given an address and port, get the partition info from that node
%%
%%      Returns the layout of the ring where the given node lives
%%
-spec partition_info(Address :: node_ip(),
                     Port :: inet:port_number()) -> {ok, ring(), unique_nodes()}
                                                  | socket_error().

partition_info(Address, Port) ->
    case gen_tcp:connect(Address, Port, ?UTIL_CONN_OPTS) of
        {error, Reason} ->
            {error, Reason};
        {ok, Sock} ->
            Reply = partition_info_internal(Sock),
            ok = gen_tcp:close(Sock),
            Reply
    end.

-spec partition_info_internal(gen_tcp:socket()) -> {ok, ring(), unique_nodes()}
                                                 | socket_error().

partition_info_internal(Socket) ->
    %% FIXME(borja): Hack to fit in message identifiers
    ok = gen_tcp:send(Socket, <<0:16, (ppb_protocol_driver:connect())/binary>>),
    case gen_tcp:recv(Socket, 0) of
        {error, Reason} ->
            {error, Reason};
        {ok, <<0:16, RawReply/binary>>} ->
            {ok, RingSize, RawRing} = pvc_proto:decode_serv_reply(RawReply),
            UniqueNodes = unique_ring_nodes(RawRing),
            FixedRing = make_fixed_ring(RingSize, RawRing),
            {ok, #ring{size=RingSize, fixed_ring=FixedRing}, UniqueNodes}
    end.

%% @doc Given an address and port, get the replica info from that node
%%
%%      Returns the layout of the ring where the given node lives,
%%      as well as the replica identifier from the cluster.
%%
-spec grb_replica_info(Address :: node_ip(),
                       Port :: inet:port_number()) -> {ok, inet:ip_address(), term(), ring(), unique_nodes()}
                                                      | socket_error().
grb_replica_info(Address, Port) ->
    grb_replica_info(Address, Port, 16).

-spec grb_replica_info(Address :: node_ip(),
                       Port :: inet:port_number(),
                       LenBits :: non_neg_integer()) -> {ok, inet:ip_address(), term(), ring(), unique_nodes()}
                                                        | socket_error().

grb_replica_info(Address, Port, LenBits) ->
    case gen_tcp:connect(Address, Port, ?UTIL_CONN_OPTS) of
        {error, Reason} ->
            {error, Reason};
        {ok, Sock} ->
            {ok, {LocalIP, _}} = inet:sockname(Sock),
            ok = gen_tcp:send(Sock, <<0:LenBits, (ppb_grb_driver:connect())/binary>>),
            Reply = case gen_tcp:recv(Sock, 0) of
                {error, Reason} ->
                    {error, Reason};
                {ok, <<0:LenBits, RawReply/binary>>} ->
                    {ok, ReplicaID, RingSize, RawRing} = pvc_proto:decode_serv_reply(RawReply),
                    UniqueNodes = unique_ring_nodes(RawRing),
                    FixedRing = make_fixed_ring(RingSize, RawRing),
                    {ok, LocalIP, ReplicaID, #ring{size=RingSize, fixed_ring=FixedRing}, UniqueNodes}
            end,
            ok = gen_tcp:close(Sock),
            Reply
    end.

-spec random_indexnode(ring()) -> index_node().
random_indexnode(#ring{size=Size, fixed_ring=Layout}) ->
    Pos = rand:uniform(Size - 1),
    erlang:element(Pos, Layout).

-spec get_key_indexnode(ring(), term()) -> index_node().
get_key_indexnode(Ring, Key) ->
    get_key_indexnode(Ring, Key, ?ANTIDOTE_BUCKET).

-spec get_key_indexnode(ring(), term(), term()) -> index_node().
get_key_indexnode(#ring{size=Size, fixed_ring=Layout}, Key, Bucket) ->
    Pos = convert_key(Key, Bucket) rem Size + 1,
    erlang:element(Pos, Layout).

%%====================================================================
%% Routing Internal functions
%%====================================================================

-spec convert_key(term(), term()) -> non_neg_integer().
convert_key(Key, Bucket) when is_binary(Key) ->
    try
        abs(binary_to_integer(Key))
    catch _:_ ->
        %% Looked into the internals of riak_core for this
        HashedKey = crypto:hash(sha, term_to_binary({Bucket, Key})),
        abs(crypto:bytes_to_integer(HashedKey))
    end;

convert_key(Key, _) when is_integer(Key) ->
    abs(Key);

convert_key(TermKey, Bucket) ->
    %% Looked into the internals of riak_core for this
    BinaryTerm = term_to_binary({Bucket, term_to_binary(TermKey)}),
    HashedKey = crypto:hash(sha, BinaryTerm),
    abs(crypto:bytes_to_integer(HashedKey)).

%%====================================================================
%% Partition Internal functions
%%====================================================================

%% @doc Get an unique list of the ring owning IP addresses
-spec unique_ring_nodes(raw_ring()) -> unique_nodes().
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
-spec index_ring(
    RawRing :: raw_ring()
) -> [{non_neg_integer(), {partition_id(), node_ip()}}].

index_ring(RawRing) ->
    index_ring(RawRing, 1, []).

index_ring([], _, Acc) ->
    lists:reverse(Acc);

index_ring([{Partition, ErlangNode} | Rest], N, Acc) ->
    Converted = {N, {Partition, erlang_node_to_ip(ErlangNode)}},
    index_ring(Rest, N + 1, [Converted | Acc]).
