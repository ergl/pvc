-module(grb_client).
-include("pvc.hrl").

%% Util API
-export([ring_size/1,
         key_location/2,
         put_conflict_information/4]).

%% Create coordinator
-export([new/3,
         new/6]).

%% Start API
-export([uniform_barrier/2,
         start_transaction/2,
         start_transaction/3,
         start_transaction/4]).

%% Sync read / update API
-export([read_key_snapshot/4,
         read_key_operation/4,
         read_key_operations/3,
         read_key_snapshots/3,
         update_operation/4,
         update_operations/3]).

%% Async read API
-export([send_read_key/4,
         receive_read_key/4,
         send_read_operation/4,
         receive_read_operation/4,
         send_read_partition/3,
         receive_read_partition/3]).

%% Async update API
-export([send_key_update/4,
         receive_key_update/4]).

%% Async simple update API (blind write)
-export([send_key_operation/4,
         receive_key_operation/4]).

%% Sync simple update API (blind write)
-export([send_key_operations/3]).

%% Commit API
-export([commit/2,
         commit_red/2,
         commit_red/3]).

%% Put a key in the writeset, without doing anything else
-export([add_keyop_to_writeset_unsafe/4]).

-type conn_pool() :: atom().
-type red_conn_pool() :: atom().

-type read_req_id() :: {read, shackle:external_request_id(), index_node()}.
-type read_op_req_id() :: {read_operation, shackle:external_request_id(), index_node()}.
-type update_req_id() :: {update, shackle:external_request_id(), operation(), index_node()}.
-type update_send_req_id() :: {update_send, shackle:external_request_id(), operation(), index_node()}.

-type read_partition_req_id() :: {read_partition, shackle:external_request_id(), index_node()}.

-record(coordinator, {
    %% The IP we're using to talk to the server
    %% Used to create a transaction id
    self_ip :: binary(),

    %% Routing info
    ring :: pvc_ring:ring(),
    %% Replica ID of the connected cluster
    replica_id = ignore :: replica_id(),

    %% Opened pool of connections, one pool per node in the cluster
    conn_pool :: #{inet:ip_address() => conn_pool()},

    %% Connection used for red commit, one pool per node in the cluster
    %% We use a separate pool of connections for red commit to increase utilization:
    %% since clients wait for a long time (cross-replica RTT), they could interfere
    %% with low-latency connections.
    red_connections :: #{inet:ip_address() => red_conn_pool()},

    coordinator_id :: non_neg_integer()
}).

-type transaction_id() :: {replica_id(), binary(), non_neg_integer(), non_neg_integer()}.
-type rvc() :: grb_vclock:vc(replica_id()).
-type read_partitions() :: #{partition_id() => true}.

-record(transaction, {
    id :: transaction_id(),
    vc = grb_vclock:new() :: rvc(),
    read_only = true :: boolean(),
    rws = pvc_grb_rws:new() :: pvc_grb_rws:t(),
    %% at which node did we start the transaction?
    start_node :: index_node(),
    %% what partitions have we read from?
    %% can optimize future reads from here
    read_partitions = #{} :: read_partitions()
}).

-opaque coord() :: #coordinator{}.
-opaque tx() :: #transaction{}.

-type key() :: term().
-type key_type() :: grb_crdt:crdt().
-type operation() :: grb_crdt:op().
-type snapshot() :: term().
-type tx_label() :: binary().

-export_type([conn_pool/0,
              red_conn_pool/0,
              coord/0,
              tx/0,
              key/0,
              key_type/0,
              operation/0,
              snapshot/0,
              tx_label/0,
              read_req_id/0,
              read_op_req_id/0,
              update_req_id/0,
              read_partition_req_id/0]).

-spec new(inet:ip_address(), inet:port_number(), non_neg_integer()) -> {ok, coord()}.
new(BootstrapIp, Port, CoordId) ->
    {ok, LocalIp, ReplicaId, Ring, Nodes} = pvc_ring:grb_replica_info(BootstrapIp, Port, 16),
    {Pools, RedConns} = lists:foldl(fun(NodeIp, {ConAcc, RedAcc}) ->
        PoolName = list_to_atom(atom_to_list(NodeIp) ++ "_shackle_pool"),
        RedPoolName = list_to_atom(atom_to_list(NodeIp) ++ "_shackle_red_pool"),
        shackle_pool:start(PoolName, pvc_shackle_transport,
                           [{address, NodeIp}, {port, Port}, {reconnect, false},
                            {socket_options, [{packet, 4}, binary, {nodelay, true}]},
                            {init_options, #{id_len => 16}}],
                           [{pool_size, 16}]),

        shackle_pool:start(RedPoolName, pvc_red_connection,
                           [{address, NodeIp}, {port, Port}, {reconnect, false},
                            {socket_options, [{packet, 4}, binary, {nodelay, true}]},
                            {init_options, #{id_len => 16}}],
                           [{pool_size, 16}]),

        {
            ConAcc#{NodeIp => PoolName},
            RedAcc#{NodeIp => RedPoolName}
        }
    end, {#{}, #{}}, Nodes),
    new(ReplicaId, LocalIp, CoordId, Ring, Pools, RedConns).

-spec new(ReplicaId :: term(),
          LocalIP :: inet:ip_address(),
          CoordId :: non_neg_integer(),
          RingInfo :: pvc_ring:ring(),
          NodePool :: #{inet:ip_address() => conn_pool()},
          RedConnections :: #{inet:ip_address() => red_conn_pool()}) -> {ok, coord()}.

new(ReplicaId, LocalIP, CoordId, RingInfo, NodePool, RedConnections) ->
    {ok, #coordinator{self_ip=list_to_binary(inet:ntoa(LocalIP)),
                      ring = RingInfo,
                      replica_id=ReplicaId,
                      conn_pool=NodePool,
                      red_connections=RedConnections,
                      coordinator_id=CoordId}}.

-spec ring_size(coord()) -> non_neg_integer().
ring_size(#coordinator{ring=Ring}) ->
    pvc_ring:size(Ring).

-spec key_location(coord(), key()) -> index_node().
key_location(#coordinator{ring=Ring}, Key) ->
    pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET).

-spec put_conflict_information(Address :: node_ip(),
                               Port :: inet:port_number(),
                               LenBits :: non_neg_integer(),
                               Conflicts :: #{tx_label() := tx_label()}) -> ok | socket_error().

put_conflict_information(Address, Port, LenBits, Conflicts) ->
    case gen_tcp:connect(Address, Port, ?UTIL_CONN_OPTS) of
        {error, Reason} ->
            {error, Reason};
        {ok, Sock} ->
            ok = gen_tcp:send(Sock, <<0:LenBits, (ppb_grb_driver:put_conflicts(Conflicts))/binary>>),
            Reply = case gen_tcp:recv(Sock, 0) of
                {error, Reason} ->
                    {error, Reason};
                {ok, <<0:LenBits, RawReply/binary>>} ->
                    ok = pvc_proto:decode_serv_reply(RawReply),
                    ok
            end,
            ok = gen_tcp:close(Sock),
            Reply
    end.

-spec add_keyop_to_writeset_unsafe(coord(), tx(), key(), operation()) -> {ok, tx()}.
add_keyop_to_writeset_unsafe(#coordinator{ring=Ring},
                             T=#transaction{rws=RWS},
                             Key,
                             Op) ->
    Idx = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    {ok, T#transaction{rws=pvc_grb_rws:add_operation(Idx, Key, Op, RWS)}}.

-spec uniform_barrier(coord(), rvc()) -> ok.
uniform_barrier(#coordinator{ring=Ring, conn_pool=Pools}, CVC) ->
    {Partition, Node} = pvc_ring:random_indexnode(Ring),
    Pool = maps:get(Node, Pools),
    ok = pvc_shackle_transport:uniform_barrier(Pool, Partition, CVC).

-spec start_transaction(coord(), non_neg_integer()) -> {ok, tx()}.
start_transaction(Coord, Id) ->
    start_transaction(Coord, Id, grb_vclock:new()).

-spec start_transaction(coord(), non_neg_integer(), rvc()) -> {ok, tx()}.
start_transaction(Coord=#coordinator{self_ip=Ip, coordinator_id=LocalId, replica_id=ReplicaId}, Id, CVC) ->
    {ok, SVC, StartNode} = start_internal(CVC, Coord),
    {ok, #transaction{id={ReplicaId, Ip, LocalId, Id}, vc=SVC, start_node=StartNode}}.

-spec start_transaction(coord(), non_neg_integer(), rvc(), index_node()) -> {ok, tx()}.
start_transaction(Coord=#coordinator{self_ip=Ip, coordinator_id=LocalId, replica_id=ReplicaId},
                  Id, CVC, StartIdx={P, N}) ->
    Pool = maps:get(N, Coord#coordinator.conn_pool),
    {ok, SVC} = pvc_shackle_transport:start_transaction(Pool, P, CVC),
    {ok, #transaction{id={ReplicaId, Ip, LocalId, Id}, vc=SVC, start_node=StartIdx}}.

-spec read_key_snapshot(coord(), tx(), key(), key_type()) -> {ok, snapshot(), tx()}.
read_key_snapshot(Coord, Tx, Key, Type) ->
    {ok, ReqId} = send_read_key(Coord, Tx, Key, Type),
    receive_read_key(Coord, Tx, Key, ReqId).

-spec read_key_operation(coord(), tx(), key(), operation()) -> {ok, term(), tx()}.
read_key_operation(Coord, Tx, Key, Operation) ->
    {ok, ReqId} = send_read_operation(Coord, Tx, Key, Operation),
    receive_read_operation(Coord, Tx, Key, ReqId).

-spec read_key_operations(coord(), tx(), [{key(), operation()}]) -> {ok, #{key() := term()}, tx()}.
read_key_operations(Coord, Tx0, KeyReadOps) ->
    KeyRequests = lists:map(fun({Key, ReadOp}) ->
        {ok, ReqId} = send_read_operation(Coord, Tx0, Key, ReadOp),
        {Key, ReqId}
    end, KeyReadOps),

    {Responses, Tx} = lists:foldl(fun({Key, ReqId}, {Responses0, Acc0}) ->
        {ok, Val, Acc} = receive_read_operation(Coord, Acc0, Key, ReqId),
        {Responses0#{Key => Val}, Acc}
    end, {#{}, Tx0}, KeyRequests),

    {ok, Responses, Tx}.

-spec read_key_snapshots(coord(), tx(), [{key(), key_type()}]) -> {ok, #{key() := snapshot()}, tx()}.
read_key_snapshots(Coord, Tx0, KeyTypes) ->
    KeyRequests = lists:map(fun({Key, Type}) ->
        {ok, ReqId} = send_read_key(Coord, Tx0, Key, Type),
        {Key, ReqId}
    end, KeyTypes),

    {Responses, Tx} = lists:foldl(fun({Key, ReqId}, {Responses0, Acc0}) ->
        {ok, Snapshot, Acc} = receive_read_key(Coord, Acc0, Key, ReqId),
        {Responses0#{Key => Snapshot}, Acc}
    end, {#{}, Tx0}, KeyRequests),

    {ok, Responses, Tx}.

-spec send_read_key(coord(), tx(), binary(), term()) -> {ok, read_req_id()}.
send_read_key(#coordinator{ring=Ring, conn_pool=Pools},
              #transaction{vc=SVC, read_partitions=ReadP, id=TxId},
              Key,
              Type) ->

    Idx={P,N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    {ok, ReqId} = pvc_shackle_transport:cast_read_request(Pool, P, TxId, SVC,
                                                          Key, Type, maps:get(P, ReadP, false)),
    {ok, {read, ReqId, Idx}}.

-spec receive_read_key(coord(), tx(), key(), read_req_id()) -> {ok, snapshot(), tx()}.
receive_read_key(_Coord,
                 Tx=#transaction{read_partitions=ReadP, rws=RWS},
                 Key,
                 {read, ReqId, Idx={P, _}}) ->
    {ok, Snapshot} = shackle:receive_response(ReqId),
    {ok, Snapshot, Tx#transaction{read_partitions=ReadP#{P => true},
                                  rws=pvc_grb_rws:add_read_key(Idx, Key, RWS)}}.

-spec send_read_operation(coord(), tx(), binary(), term()) -> {ok, read_op_req_id()}.
send_read_operation(#coordinator{ring=Ring, conn_pool=Pools},
                    #transaction{vc=SVC, read_partitions=ReadP, id=TxId},
                    Key,
                    Operation) ->

    Idx={P,N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    {ok, ReqId} = pvc_shackle_transport:cast_read_operation(Pool, P, TxId, SVC,
                                                            Key, Operation, maps:get(P, ReadP, false)),
    {ok, {read_operation, ReqId, Idx}}.

-spec receive_read_operation(coord(), tx(), key(), read_op_req_id()) -> {ok, term(), tx()}.
receive_read_operation(_Coord,
                        Tx=#transaction{read_partitions=ReadP, rws=RWS},
                        Key,
                        {read_operation, ReqId, Idx={P, _}}) ->

    {ok, Value} = shackle:receive_response(ReqId),
    {ok, Value, Tx#transaction{read_partitions=ReadP#{P => true},
                               rws=pvc_grb_rws:add_read_key(Idx, Key, RWS)}}.

-spec send_read_partition(coord(), tx(), [{key(), key_type()}]) -> {ok, read_partition_req_id()}.
send_read_partition(#coordinator{ring=Ring, conn_pool=Pools},
                    #transaction{vc=SVC, read_partitions=ReadP, id=TxId},
                    [ {FKey, _} | _ ]=KeyTypes) ->

    Idx={P,N} = pvc_ring:get_key_indexnode(Ring, FKey, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    ReadAgain = maps:get(P, ReadP, false),
    {ok, ReqId} = pvc_shackle_transport:cast_partition_read_request(Pool, P, TxId, SVC, ReadAgain, KeyTypes),
    {ok, {read_partition, ReqId, Idx}}.

-spec receive_read_partition(coord(), tx(), read_partition_req_id()) -> {ok, #{key() := snapshot()}, tx()}.
receive_read_partition(_Coord, Tx0, {read_partition, ReqId, Idx}) ->

    {ok, Responses} = shackle:receive_response(ReqId),

    Tx = maps:fold(fun(Key, _, TxAcc=#transaction{rws=RWS}) ->
        TxAcc#transaction{rws=pvc_grb_rws:add_read_key(Idx, Key, RWS)}
    end, Tx0, Responses),

    {ok, Responses, Tx}.

-spec update_operation(coord(), tx(), key(), operation()) -> {ok, snapshot(), tx()}.
update_operation(Coord, Tx, Key, Operation) ->
    {ok, ReqId} = send_key_update(Coord, Tx, Key, Operation),
    receive_key_update(Coord, Tx, Key, ReqId).

-spec update_operations(coord(), tx(), [{key(), operation()}]) -> {ok, #{key() := snapshot()}, tx()}.
update_operations(Coord, Tx0, KeyOps) ->
    KeyRequests = lists:map(fun({Key, Operation}) ->
        {ok, ReqId} = send_key_update(Coord, Tx0, Key, Operation),
        {Key, ReqId}
    end, KeyOps),

    {Responses, Tx} = lists:foldl(fun({Key, ReqId}, {Responses0, Acc0}) ->
        {ok, Snapshot, Acc} = receive_key_update(Coord, Acc0, Key, ReqId),
        {Responses0#{Key => Snapshot}, Acc}
    end, {#{}, Tx0}, KeyRequests),

    {ok, Responses, Tx}.

-spec send_key_update(coord(), tx(), key(), operation()) -> {ok, update_req_id()}.
send_key_update(#coordinator{ring=Ring, conn_pool=Pools},
                #transaction{vc=SVC, read_partitions=ReadP, id=TxId},
                Key,
                Operation) ->

    Idx={P,N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    {ok, ReqId} = pvc_shackle_transport:cast_update_request(Pool, P, TxId, SVC,
                                                            Key, Operation, maps:get(P, ReadP, false)),
    {ok, {update, ReqId, Operation, Idx}}.

-spec receive_key_update(coord(), tx(), key(), update_req_id()) -> {ok, snapshot(), tx()}.
receive_key_update(_Coord,
                   Tx=#transaction{read_partitions=ReadP, rws=RWS},
                   Key,
                   {update, ReqId, Op, Idx={P, _}}) ->

    {ok, Snapshot} = shackle:receive_response(ReqId),
    {ok, Snapshot, Tx#transaction{read_only=false,
                                 read_partitions=ReadP#{P => true},
                                 rws=pvc_grb_rws:add_operation(Idx, Key, Op, RWS)}}.

-spec send_key_operation(coord(), tx(), key(), operation()) -> {ok, update_send_req_id()}.
send_key_operation(#coordinator{ring=Ring, conn_pool=Pools},
                   #transaction{id=TxId},
                   Key,
                   Operation) ->
    Idx={P,N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    {ok, ReqId} = pvc_shackle_transport:cast_update_send(Pool, P, TxId, Key, Operation),
    {ok, {update_send, ReqId, Operation, Idx}}.

-spec receive_key_operation(coord(), tx(), key(), update_send_req_id()) -> {ok, tx()}.
receive_key_operation(_Coord,
                      Tx=#transaction{rws=RWS},
                      Key,
                      {update_send, ReqId, Operation, Idx}) ->

    ok = shackle:receive_response(ReqId),
    {ok, Tx#transaction{read_only=false,
                        rws=pvc_grb_rws:add_operation(Idx, Key, Operation, RWS)}}.

-spec send_key_operations(coord(), tx(), [{key(), operation()}]) -> {ok, tx()}.
send_key_operations(Coord, Tx0, KeyOps) ->
    KeyReqs = [
        {K, element(2, send_key_operation(Coord, Tx0, K, O))}
        || {K, O} <- KeyOps
    ],
    Tx1 = lists:foldl(fun({K, Id}, TxAcc0) ->
        {ok, TxAcc} = receive_key_operation(Coord, TxAcc0, K, Id),
        TxAcc
    end, Tx0, KeyReqs),
    {ok, Tx1}.

-spec commit(coord(), tx()) -> rvc().
commit(_, #transaction{read_only=true, vc=SVC}) -> SVC;
commit(Coord, Tx) -> commit_internal(Coord, Tx).

-spec commit_red(coord(), tx()) -> {ok, rvc()} | {abort, term()}.
commit_red(Coord, Tx) ->
    commit_red(Coord, Tx, <<"default">>).

-spec commit_red(coord(), tx(), tx_label()) -> {ok, rvc()} | {abort, term()}.
commit_red(Coord, Tx, Label) ->
    #coordinator{red_connections=RedConns} = Coord,
    #transaction{rws=RWS, id=TxId, vc=SVC, start_node={Partition, CoordNode}} = Tx,
    Pool = maps:get(CoordNode, RedConns),
    Prepares = pvc_grb_rws:make_red_prepares(RWS),
    pvc_red_connection:commit_red(Pool, Partition, TxId, Label, SVC, Prepares).

%%====================================================================
%% Read Internal functions
%%====================================================================

-spec start_internal(rvc(), coord()) -> {ok, rvc(), index_node()}.
start_internal(CVC, #coordinator{ring=Ring, conn_pool=Pools}) ->
    Idx={P, N} = pvc_ring:random_indexnode(Ring),
    Pool = maps:get(N, Pools),
    {ok, SVC} = pvc_shackle_transport:start_transaction(Pool, P, CVC),
    {ok, SVC, Idx}.

-spec commit_internal(coord(), tx()) -> rvc().
commit_internal(Coord, Tx) ->
    {Nodes, CVC} = prepare_blue(Coord, Tx),
    ok = decide_blue(Tx#transaction.id, CVC, Nodes, Coord),
    CVC.

-spec prepare_blue(coord(), tx()) -> {#{node_ip() => [partition_id()]}, rvc()}.
prepare_blue(#coordinator{conn_pool=Pools, replica_id=ReplicaId}, Tx) ->
    #transaction{rws=RWS, id=TxId, vc=SVC} = Tx,
    {Requests, Nodes} = pvc_grb_rws:fold_updated_partitions(
        fun(Node, Partitions, {ReqAcc, NodeAcc}) ->
            Pool = maps:get(Node, Pools),
            {ok, ReqId} = pvc_shackle_transport:prepare_blue(Pool, TxId, SVC, Partitions),
            {
                [ReqId | ReqAcc],
                NodeAcc#{Node => Partitions}
            }
        end,
        {[], #{}},
        RWS
    ),

    {Nodes, collect(Requests, ReplicaId, SVC)}.

-spec collect([inet:socket()], term(), rvc()) -> rvc().
collect([], _, CVC) -> CVC;
collect([ReqId | Rest], ReplicaId, CVC) ->
    Votes = shackle:receive_response(ReqId),
    collect(Rest, ReplicaId, update_vote(Votes, ReplicaId, CVC)).

-spec update_vote([{ok, partition_id(), non_neg_integer()}, ...], replica_id(), rvc()) -> rvc().
update_vote(Votes, ReplicaId, CVC) ->
    lists:foldl(fun({ok, _, PT}, Acc) ->
        grb_vclock:set_time(ReplicaId,
                            erlang:max(PT, grb_vclock:get_time(ReplicaId, Acc)),
                            Acc)
    end, CVC, Votes).

-spec decide_blue(transaction_id(), rvc(), #{node_ip() => [partition_id()]}, coord()) -> ok.
decide_blue(TxId, CVC, Nodes, #coordinator{conn_pool=Pools}) ->
    maps:fold(fun(Node, Partitions, ok) ->
        Pool = maps:get(Node, Pools),
        pvc_shackle_transport:decide_blue(Pool, TxId, Partitions, CVC)
    end, ok, Nodes).
