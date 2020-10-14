-module(grb_client).

-include("pvc.hrl").

%% API
-export([new/6,
         uniform_barrier/2,
         start_transaction/2,
         start_transaction/3,
         read_op/3,
         update_op/4,
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
    rws = pvc_grb_rws:new() :: pvc_grb_rws:t()
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
    {ok, SVC} = start_internal(CVC, Coord),
    {ok, #transaction{id={Ip, LocalId, Id}, vc=SVC}}.

%% todo(borja): parallel read
-spec read_op(coord(), tx(), term()) -> {ok, term(), tx()}.
read_op(Coord, Tx=#transaction{rws=RWS, vc=SVC}, Key) ->
    {Val, NewRWS} = op_internal(Coord, Key, SVC, RWS),
    {ok, Val, Tx#transaction{rws=NewRWS}}.

%% todo(borja): parallel update
update_op(Coord, Tx=#transaction{rws=RWS, vc=SVC}, Key, Val) ->
    {NewVal, NewRWS} = op_internal(Coord, Key, Val, SVC, RWS),
    {ok, NewVal, Tx#transaction{rws=NewRWS, read_only=false}}.

-spec commit(coord(), tx()) -> rvc().
commit(_, #transaction{read_only=true, vc=SVC}) -> SVC;
commit(Coord, Tx) -> commit_internal(Coord, Tx).

-spec commit_red(coord(), tx()) -> {ok, rvc()} | {abort, term()}.
commit_red(Coord, Tx) ->
    #coordinator{red_connections=RedConns, ring=Ring, coordinator_id=Id} = Coord,
    #transaction{rws=RWS, id=TxId, vc=SVC} = Tx,
    {_, CoordNode} = pvc_ring:random_indexnode(Ring),
    ConnHandle = maps:get(CoordNode, RedConns),
    Prepares = pvc_grb_rws:make_red_prepares(RWS),
    pvc_red_connection:commit_red(ConnHandle, Id, TxId, SVC, Prepares).

%%====================================================================
%% Read Internal functions
%%====================================================================

start_internal(CVC, #coordinator{coordinator_id=Id, ring=Ring, conn_pool=Pools}) ->
    {P, N} = pvc_ring:random_indexnode(Ring),
    Pool = maps:get(N, Pools),
    pvc_shackle_transport:start_transaction(Pool, Id, P, CVC).

-spec op_internal(coord(), term(), rvc(), pvc_grb_rws:t()) -> {term(), pvc_grb_rws:t()}.
op_internal(Coord, Key, SVC, RWS) ->
    {Idx, NewVal} = send_op_internal(Coord, Key, <<>>, SVC),
    {NewVal, pvc_grb_rws:put_ronly_op(Idx, Key, RWS)}.

-spec op_internal(coord(), term(), term(), rvc(), pvc_grb_rws:t()) -> {term(), pvc_grb_rws:t()}.
op_internal(Coord, Key, Val, SVC, RWS) ->
    {Idx, NewVal} = send_op_internal(Coord, Key, Val, SVC),
    {NewVal, pvc_grb_rws:put_op(Idx, Key, Val, RWS)}.

-spec send_op_internal(coord(), term(), term(), rvc()) -> {index_node(), term()}.
send_op_internal(#coordinator{ring=Ring, coordinator_id=Id, conn_pool=Pools}, Key, Val, SVC) ->
    Idx={P, N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    {ok, NewVal} = pvc_shackle_transport:op_request(Pool, Id, P, SVC, Key, Val),
    {Idx, NewVal}.

-spec commit_internal(coord(), tx()) -> rvc().
commit_internal(Coord, Tx) ->
    CVC = prepare_blue(Coord, Tx),
    ok = decide_blue(Coord, Tx, CVC),
    CVC.

-spec prepare_blue(coord(), tx()) -> rvc().
prepare_blue(#coordinator{conn_pool=Pools, coordinator_id=Id, replica_id=RId}, Tx) ->
    #transaction{rws=RWS, id=TxId, vc=SVC} = Tx,
    ReqIds = pvc_grb_rws:fold(fun(Node, Partitions, ReqAcc) ->
        Pool = maps:get(Node, Pools),
        Prepares = maps:fold(fun(Partition, {_, WS}, Acc) ->
            [{Partition, WS} | Acc]
        end, [], Partitions),
        {ok, ReqId} = pvc_shackle_transport:prepare_blue(Pool, Id, TxId, SVC, Prepares),
        [ReqId | ReqAcc]
    end, [], RWS),
    collect(ReqIds, Id, RId, SVC).

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

-spec decide_blue(coord(), tx(), rvc()) -> ok.
decide_blue(#coordinator{conn_pool=Pools, coordinator_id=Id}, Tx, CVC) ->
    #transaction{rws=RWS, id=TxId} = Tx,
    ok = pvc_grb_rws:fold(fun(Node, Partitions, _) ->
        Pool = maps:get(Node, Pools),
        DecidePartitions = maps:keys(Partitions),
        ok = pvc_shackle_transport:decide_blue(Pool, Id, TxId, DecidePartitions, CVC)
    end, ok, RWS).
