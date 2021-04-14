-module(pvc_ring).

-include("pvc.hrl").

%% API
-export([grb_replica_info/3,
         random_indexnode/1,
         get_key_indexnode/3,
         size/1]).


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

%% @doc Given an address and port, get the replica info from that node
%%
%%      Returns the layout of the ring where the given node lives,
%%      as well as the replica identifier from the cluster.
%%

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

-spec get_key_indexnode(ring(), term(), term()) -> index_node().
get_key_indexnode(#ring{size=Size, fixed_ring=Layout}, Key, Bucket) ->
    Pos = convert_key(Key, Bucket) rem Size + 1,
    erlang:element(Pos, Layout).

-spec size(ring()) -> non_neg_integer().
size(#ring{size=Size}) ->
    Size.

%%====================================================================
%% Routing Internal functions
%%====================================================================

-spec convert_key(term(), term()) -> non_neg_integer().
convert_key(Key, Bucket) ->
    if
        is_integer(Key) -> convert_key_int(Key);
        is_binary(Key) -> convert_key_binary(Key, Bucket);
        is_tuple(Key) -> convert_key(element(1, Key), Bucket);
        true -> convert_key_hash(Key, Bucket)
    end.

-spec convert_key_int(integer()) -> non_neg_integer().
convert_key_int(Int) ->
    abs(Int).

-spec convert_key_binary(binary(), term()) -> non_neg_integer().
convert_key_binary(Bin, Bucket) ->
    AsInt = (catch list_to_integer(binary_to_list(Bin))),
    if
        is_integer(AsInt) ->
            convert_key_int(AsInt);
        true ->
            convert_key_hash(Bin, Bucket)
    end.

-spec convert_key_hash(term(), term()) -> non_neg_integer().
convert_key_hash(Key, Bucket) ->
    %% Looked into the internals of riak_core for this
    HashKey = crypto:hash(sha, term_to_binary({Bucket, Key})),
    abs(crypto:bytes_to_integer(HashKey)).

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
