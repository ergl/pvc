-module(grb_client).
-include("pvc.hrl").

%% API
-export([new/6,
         uniform_barrier/2,
         start_transaction/2,
         start_transaction/3,
         start_read/3,
         start_read/4,
         read_op/3,
         read_bypass/3,
         update_op/4,
         update_bypass/4,
         commit/2,
         commit_red/2]).

-type conn_pool() :: atom().

-record(coordinator, {
    %% The IP we're using to talk to the server
    %% Used to create a transaction id
    self_ip :: binary(),

    %% Routing info
    ring :: pvc_ring:ring(),
    %% Replica ID of the connected cluster
    replica_id = ignore :: replica_id(),

    %% Opened connection, one per node in the cluster
    conn_pool :: #{inet:ip_address() => conn_pool()},
    %% Connection used for red commit, one per node in the cluster
    %% We use a separate connection for red commit to increase utilization:
    %% since clients wait for a long time (cross-replica RTT), it's enough to
    %% have a single connection.
    red_connections :: #{inet:ip_address() => pvc_red_connection:t()},

    coordinator_id :: non_neg_integer()
}).

-type transaction_id() :: {binary(), non_neg_integer(), non_neg_integer()}.
-type rvc() :: pvc_vclock:vc(replica_id()).

-record(transaction, {
    id :: transaction_id(),
    vc = pvc_vclock:new() :: rvc(),
    read_only = true :: boolean(),
    rws = pvc_grb_rws:new() :: pvc_grb_rws:t(),
    %% at which node did we start the transaction?
    start_node :: index_node()
}).

-opaque coord() :: #coordinator{}.
-opaque tx() :: #transaction{}.

-export_type([conn_pool/0, coord/0, tx/0]).

-spec new(ReplicaId :: term(),
          LocalIP :: inet:ip_address(),
          CoordId :: non_neg_integer(),
          RingInfo :: pvc_ring:ring(),
          NodePool :: #{inet:ip_address() => conn_pool()},
          RedConnections :: #{inet:ip_address() => pvc_red_connection:t()}) -> {ok, coord()}.

new(ReplicaId, LocalIP, CoordId, RingInfo, NodePool, RedConnections) ->
    {ok, #coordinator{self_ip=list_to_binary(inet:ntoa(LocalIP)),
                      ring = RingInfo,
                      replica_id=ReplicaId,
                      conn_pool=NodePool,
                      red_connections=RedConnections,
                      coordinator_id=CoordId}}.

-spec uniform_barrier(coord(), rvc()) -> ok.
uniform_barrier(#coordinator{coordinator_id=Id, ring=Ring, conn_pool=Pools}, CVC) ->
    {Partition, Node} = pvc_ring:random_indexnode(Ring),
    Pool = maps:get(Node, Pools),
    ok = pvc_shackle_transport:uniform_barrier(Pool, Id, Partition, CVC).

-spec start_transaction(coord(), non_neg_integer()) -> {ok, tx()}.
start_transaction(Coord, Id) ->
    start_transaction(Coord, Id, pvc_vclock:new()).

-spec start_transaction(coord(), non_neg_integer(), rvc()) -> {ok, tx()}.
start_transaction(Coord=#coordinator{self_ip=Ip, coordinator_id=LocalId}, Id, CVC) ->
    {ok, SVC, StartNode} = start_internal(CVC, Coord),
    {ok, #transaction{id={Ip, LocalId, Id}, vc=SVC, start_node=StartNode}}.

-spec start_read(coord(), non_neg_integer(), binary()) -> {ok, binary(), tx()}.
start_read(Coord, Id, Key) ->
    start_read(Coord, Id, Key, pvc_vclock:new()).

-spec start_read(coord(), non_neg_integer(), binary(), rvc()) -> {ok, binary(), tx()}.
start_read(Coord=#coordinator{self_ip=Ip, coordinator_id=LocalId}, Id, Key, CVC) ->
    {ok, Val, SVC, StartNode} = start_read_internal(Key, CVC, Coord),
    Tx = #transaction{id={Ip, LocalId, Id}, vc=SVC, start_node=StartNode},
    {ok, Val, Tx#transaction{rws=pvc_grb_rws:put_ronly_op(StartNode, Key, Tx#transaction.rws)}}.

%% todo(borja): parallel read
-spec read_op(coord(), tx(), binary()) -> {ok, binary(), tx()}.
read_op(Coord, Tx=#transaction{rws=RWS, vc=SVC}, Key) ->
    {Val, NewRWS} = read_op_internal(Coord, Key, SVC, RWS),
    {ok, Val, Tx#transaction{rws=NewRWS}}.

%% todo(borja): parallel read, add when reading partition again
-spec read_bypass(coord(), tx(), binary()) -> {ok, binary(), tx()}.
read_bypass(Coord, Tx=#transaction{rws=RWS, vc=SVC}, Key) ->
    {Val, NewRWS} = read_bypass_internal(Coord, Key, SVC, RWS),
    {ok, Val, Tx#transaction{rws=NewRWS}}.

%% todo(borja): parallel update
update_op(Coord, Tx=#transaction{rws=RWS, vc=SVC}, Key, Val) ->
    {NewVal, NewRWS} = update_op_internal(Coord, Key, Val, SVC, RWS),
    {ok, NewVal, Tx#transaction{rws=NewRWS, read_only=false}}.

update_bypass(Coord, Tx=#transaction{rws=RWS, vc=SVC}, Key, Val) ->
    {NewVal, NewRWS} = update_bypass_internal(Coord, Key, Val, SVC, RWS),
    {ok, NewVal, Tx#transaction{rws=NewRWS, read_only=false}}.

-spec commit(coord(), tx()) -> rvc().
commit(_, #transaction{read_only=true, vc=SVC}) -> SVC;
commit(Coord, Tx) -> commit_internal(Coord, Tx).

-spec commit_red(coord(), tx()) -> {ok, rvc()} | {abort, term()}.
commit_red(Coord, Tx) ->
    #coordinator{red_connections=RedConns, coordinator_id=Id} = Coord,
    #transaction{rws=RWS, id=TxId, vc=SVC, start_node={Partition, CoordNode}} = Tx,
    ConnHandle = maps:get(CoordNode, RedConns),
    Prepares = pvc_grb_rws:make_red_prepares(RWS),
    pvc_red_connection:commit_red(ConnHandle, Id, Partition, TxId, SVC, Prepares).

%%====================================================================
%% Read Internal functions
%%====================================================================

-spec start_internal(rvc(), coord()) -> {ok, rvc(), index_node()}.
start_internal(CVC, #coordinator{coordinator_id=Id, ring=Ring, conn_pool=Pools}) ->
    Idx={P, N} = pvc_ring:random_indexnode(Ring),
    Pool = maps:get(N, Pools),
    {ok, SVC} = pvc_shackle_transport:start_transaction(Pool, Id, P, CVC),
    {ok, SVC, Idx}.

start_read_internal(Key, CVC, #coordinator{coordinator_id=Id, ring=Ring, conn_pool=Pools}) ->
    Idx={P, N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    {ok, Snapshot, SVC} = pvc_shackle_transport:start_read(Pool, Id, P, CVC, Key),
    {ok, Snapshot, SVC, Idx}.

-spec read_op_internal(coord(), binary(), rvc(), pvc_grb_rws:t()) -> {binary(), pvc_grb_rws:t()}.
read_op_internal(Coord, Key, SVC, RWS) ->
    {Idx, Snapshot} = send_vsn_request(Coord, Key, SVC),
    {Snapshot, pvc_grb_rws:put_ronly_op(Idx, Key, RWS)}.

-spec read_bypass_internal(coord(), binary(), rvc(), pvc_grb_rws:t()) -> {binary(), pvc_grb_rws:t()}.
read_bypass_internal(Coord, Key, SVC, RWS) ->
    {Idx, Snapshot} = send_vsn_request_again(Coord, Key, SVC),
    {Snapshot, pvc_grb_rws:put_ronly_op(Idx, Key, RWS)}.

-spec update_op_internal(coord(), binary(), binary(), rvc(), pvc_grb_rws:t()) -> {binary(), pvc_grb_rws:t()}.
update_op_internal(Coord, Key, Val, SVC, RWS) ->
    %% todo(borja, crdts): Apply given operation to snapshot
    {Idx, _} = send_vsn_request(Coord, Key, SVC),
    {Val, pvc_grb_rws:put_op(Idx, Key, Val, RWS)}.

-spec update_bypass_internal(coord(), binary(), binary(), rvc(), pvc_grb_rws:t()) -> {binary(), pvc_grb_rws:t()}.
update_bypass_internal(Coord, Key, Val, SVC, RWS) ->
    %% todo(borja, crdts): Apply given operation to snapshot
    {Idx, _} = send_vsn_request_again(Coord, Key, SVC),
    {Val, pvc_grb_rws:put_op(Idx, Key, Val, RWS)}.

-spec send_vsn_request(coord(), binary(), rvc()) -> {index_node(), binary()}.
send_vsn_request(#coordinator{ring=Ring, coordinator_id=Id, conn_pool=Pools}, Key, SVC) ->
    Idx={P, N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    {ok, Snapshot} = pvc_shackle_transport:get_key_version(Pool, Id, P, SVC, Key),
    {Idx, Snapshot}.

-spec send_vsn_request_again(coord(), binary(), rvc()) -> {index_node(), binary()}.
send_vsn_request_again(#coordinator{ring=Ring, coordinator_id=Id, conn_pool=Pools}, Key, SVC) ->
    Idx={P, N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    {ok, Snapshot} = pvc_shackle_transport:get_key_version_again(Pool, Id, P, SVC, Key),
    {Idx, Snapshot}.

-spec commit_internal(coord(), tx()) -> rvc().
commit_internal(Coord, Tx) ->
    {Nodes, CVC} = prepare_blue(Coord, Tx),
    ok = decide_blue(Tx#transaction.id, CVC, Nodes, Coord),
    CVC.

-spec prepare_blue(coord(), tx()) -> {#{node_ip() => [partition_id()]}, rvc()}.
prepare_blue(#coordinator{conn_pool=Pools, coordinator_id=Id, replica_id=ReplicaId}, Tx) ->
    #transaction{rws=RWS, id=TxId, vc=SVC} = Tx,
    {Requests, Nodes} = pvc_grb_rws:fold_updates(
        fun(Node, Partitions, Prepares, {ReqAcc, NodeAcc}) ->
            Pool = maps:get(Node, Pools),
            {ok, ReqId} = pvc_shackle_transport:prepare_blue(Pool, Id, TxId, SVC, Prepares),
            {
                [ReqId | ReqAcc],
                NodeAcc#{Node => Partitions}
            }
        end,
        {[], #{}},
        RWS
    ),

    {Nodes, collect(Requests, Id, ReplicaId, SVC)}.

-spec collect([inet:socket()], non_neg_integer(), term(), rvc()) -> rvc().
collect([], _, _, CVC) -> CVC;
collect([ReqId | Rest], Id, ReplicaId, CVC) ->
    Votes = shackle:receive_response(ReqId),
    collect(Rest, Id, ReplicaId, update_vote(Votes, ReplicaId, CVC)).

-spec update_vote([{ok, partition_id(), non_neg_integer()}, ...], replica_id(), rvc()) -> rvc().
update_vote(Votes, ReplicaId, CVC) ->
    lists:foldl(fun({ok, _, PT}, Acc) ->
        pvc_vclock:set_time(ReplicaId,
                            erlang:max(PT, pvc_vclock:get_time(ReplicaId, Acc)),
                            Acc)
    end, CVC, Votes).

-spec decide_blue(transaction_id(), rvc(), #{node_ip() => [partition_id()]}, coord()) -> ok.
decide_blue(TxId, CVC, Nodes, #coordinator{conn_pool=Pools, coordinator_id=Id}) ->
    maps:fold(fun(Node, Partitions, ok) ->
        Pool = maps:get(Node, Pools),
        pvc_shackle_transport:decide_blue(Pool, Id, TxId, Partitions, CVC)
    end, ok, Nodes).
