-module(grb_client).
-include("pvc.hrl").

%% Util API
-export([put_conflict_information/4]).

%% Create coordinator
-export([new/3,
         new/6]).

%% Transactional API
-export([uniform_barrier/2,
         start_transaction/2,
         start_transaction/3,
         read_key_snapshot/4,
         update_lww/4,
         update_gset/4,
         update_gcounter/4,
         update_maxtuple/4,
         update_operation/5,
         commit/2,
         commit_red/2,
         commit_red/3]).

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

-export_type([conn_pool/0, coord/0, tx/0]).

-spec new(inet:ip_address(), inet:port_number(), non_neg_integer()) -> {ok, coord()}.
new(BootstrapIp, Port, CoordId) ->
    {ok, LocalIp, ReplicaId, Ring, Nodes} = pvc_ring:grb_replica_info(BootstrapIp, Port),
    {Pools, RedConns} = lists:foldl(fun(NodeIp, {ConAcc, RedAcc}) ->
        PoolName = list_to_atom(atom_to_list(NodeIp) ++ "_shackle_pool"),
        shackle_pool:start(PoolName, pvc_shackle_transport,
                           [{address, NodeIp}, {port, Port}, {reconnect, false},
                            {socket_options, [{packet, 4}, binary, {nodelay, true}]},
                            {init_options, #{id_len => 16}}],
                           [{pool_size, 16}]),

        {ok, H} = pvc_red_connection:start_connection(NodeIp, Port, 16),
        {
            ConAcc#{NodeIp => PoolName},
            RedAcc#{NodeIp => H}
        }
    end, {#{}, #{}}, Nodes),
    new(ReplicaId, LocalIp, CoordId, Ring, Pools, RedConns).

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

-spec put_conflict_information(Address :: node_ip(),
                               Port :: inet:port_number(),
                               LenBits :: non_neg_integer(),
                               Concflicts :: #{binary() := binary()}) -> ok | socket_error().

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

-spec read_key_snapshot(coord(), tx(), binary(), term()) -> {ok, term(), tx()}.
read_key_snapshot(#coordinator{ring=Ring, coordinator_id=Id, conn_pool=Pools},
                  Tx=#transaction{rws=RWS, vc=SVC, read_partitions=ReadP, id=TxId},
                  Key, Type) ->

    Idx={P, N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    {ok, Snapshot} = pvc_shackle_transport:read_request(Pool, Id, P, TxId, SVC,
                                                        Key, Type, maps:get(P, ReadP, false)),

    {ok, Snapshot, Tx#transaction{read_partitions=ReadP#{P => true},
                                  rws=pvc_grb_rws:add_read_key(Idx, Key, RWS)}}.

-spec update_lww(coord(), tx(), binary(), term()) -> {ok, term(), tx()}.
update_lww(Coord, Tx, Key, Val) ->
    update_operation(Coord, Tx, Key, grb_lww, Val).

-spec update_gset(coord(), tx(), binary(), term()) -> {ok, term(), tx()}.
update_gset(Coord, Tx, Key, Val) ->
    update_operation(Coord, Tx, Key, grb_gset, Val).

-spec update_gcounter(coord(), tx(), binary(), non_neg_integer()) -> {ok, non_neg_integer(), tx()}.
update_gcounter(Coord, Tx, Key, Incr) ->
    update_operation(Coord, Tx, Key, grb_gcounter, Incr).

-spec update_maxtuple(coord(), tx(), binary(), {non_neg_integer(), term()}) -> {ok, {non_neg_integer(), term()}, tx()}.
update_maxtuple(Coord, Tx, Key, {S, Val}) ->
    update_operation(Coord, Tx, Key, grb_maxtuple, {S, Val}).

-spec update_operation(coord(), tx(), binary(), module(), term()) -> {ok, term(), tx()}.
update_operation(#coordinator{ring=Ring, coordinator_id=Id, conn_pool=Pools},
                 Tx=#transaction{rws=RWS, vc=SVC, read_partitions=ReadP, id=TxId},
                 Key,
                 Mod,
                 Val) ->

    Operation = grb_crdt:make_op(Mod, Val),
    Idx={P, N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    Pool = maps:get(N, Pools),
    {ok, Snapshot} = pvc_shackle_transport:update_request(Pool, Id, P, TxId, SVC,
                                                          Key, Operation, maps:get(P, ReadP, false)),
    {ok, Snapshot, Tx#transaction{read_only=false,
                                  read_partitions=ReadP#{P => true},
                                  rws=pvc_grb_rws:add_operation(Idx, Key, Operation, RWS)}}.

-spec commit(coord(), tx()) -> rvc().
commit(_, #transaction{read_only=true, vc=SVC}) -> SVC;
commit(Coord, Tx) -> commit_internal(Coord, Tx).

-spec commit_red(coord(), tx()) -> {ok, rvc()} | {abort, term()}.
commit_red(Coord, Tx) ->
    commit_red(Coord, Tx, <<"default">>).

-spec commit_red(coord(), tx(), binary()) -> {ok, rvc()} | {abort, term()}.
commit_red(Coord, Tx, Label) ->
    #coordinator{red_connections=RedConns, coordinator_id=Id} = Coord,
    #transaction{rws=RWS, id=TxId, vc=SVC, start_node={Partition, CoordNode}} = Tx,
    ConnHandle = maps:get(CoordNode, RedConns),
    Prepares = pvc_grb_rws:make_red_prepares(RWS),
    pvc_red_connection:commit_red(ConnHandle, Id, Partition, TxId, Label, SVC, Prepares).

%%====================================================================
%% Read Internal functions
%%====================================================================

-spec start_internal(rvc(), coord()) -> {ok, rvc(), index_node()}.
start_internal(CVC, #coordinator{coordinator_id=Id, ring=Ring, conn_pool=Pools}) ->
    Idx={P, N} = pvc_ring:random_indexnode(Ring),
    Pool = maps:get(N, Pools),
    {ok, SVC} = pvc_shackle_transport:start_transaction(Pool, Id, P, CVC),
    {ok, SVC, Idx}.

-spec commit_internal(coord(), tx()) -> rvc().
commit_internal(Coord, Tx) ->
    {Nodes, CVC} = prepare_blue(Coord, Tx),
    ok = decide_blue(Tx#transaction.id, CVC, Nodes, Coord),
    CVC.

-spec prepare_blue(coord(), tx()) -> {#{node_ip() => [partition_id()]}, rvc()}.
prepare_blue(#coordinator{conn_pool=Pools, coordinator_id=Id, replica_id=ReplicaId}, Tx) ->
    #transaction{rws=RWS, id=TxId, vc=SVC} = Tx,
    {Requests, Nodes} = pvc_grb_rws:fold_updated_partitions(
        fun(Node, Partitions, {ReqAcc, NodeAcc}) ->
            Pool = maps:get(Node, Pools),
            {ok, ReqId} = pvc_shackle_transport:prepare_blue(Pool, Id, TxId, SVC, Partitions),
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
