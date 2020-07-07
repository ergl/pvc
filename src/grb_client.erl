-module(grb_client).

-include("pvc.hrl").

%% API
-export([new/5,
         close/1,
         uniform_barrier/2,
         start_transaction/2,
         start_transaction/3,
         read_op/3,
         update_op/4,
         commit/2]).


-record(coordinator, {
    %% The IP we're using to talk to the server
    %% Used to create a transaction id
    self_ip :: binary(),

    %% Routing info
    ring :: pvc_ring:ring(),
    %% Replica ID of the connected cluster
    replica_id = ignore :: replica_id(),

    %% Opened connection, one per node in the cluster
    sockets :: #{inet:ip_address() => inet:socket()},

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

-export_type([coord/0, tx/0]).

-spec new(ReplicaId :: term(),
          CoordId :: non_neg_integer(),
          RingInfo :: pvc_ring:ring(),
          Nodes :: [inet:ip_address()],
          Port :: inet:port_number()) -> {ok, coord()}.
new(ReplicaId, CoordId, RingInfo, Nodes, Port) ->

    Sockets = lists:foldl(fun(Node, Acc) ->
        {ok, S} = gen_tcp:connect(Node, Port, [{active, false}, {packet, 4}, binary]),
        Acc#{Node => S}
    end, #{}, Nodes),

    [Main | _] = Nodes,
    S = maps:get(Main, Sockets),
    {ok, {LocalIP, _}} = inet:sockname(S),

    {ok, #coordinator{self_ip=list_to_binary(inet:ntoa(LocalIP)),
                      ring = RingInfo,
                      replica_id=ReplicaId,
                      sockets=Sockets,
                      coordinator_id=CoordId}}.

-spec close(coord()) -> ok.
close(#coordinator{sockets=Sockets}) ->
    [ gen_tcp:close(S) || {_, S} <- maps:to_list(Sockets) ],
    ok.

-spec uniform_barrier(coord(), rvc()) -> ok.
uniform_barrier(#coordinator{coordinator_id=Id, ring=Ring, sockets=Socks}, CVC) ->
    {Partition, Node} = pvc_ring:random_indexnode(Ring),
    S = maps:get(Node, Socks),
    ok = gen_tcp:send(S, <<Id:16, (ppb_grb_driver:uniform_barrier(Partition, CVC))/binary>>),
    case gen_tcp:recv(S, 0) of
        {error, Reason} ->
            {error, Reason};
        {ok, <<Id:16, RawReply/binary>>} ->
            pvc_proto:decode_serv_reply(RawReply)
    end.

-spec start_transaction(coord(), non_neg_integer()) -> {ok, tx()}.
start_transaction(Coord, Id) ->
    start_transaction(Coord, Id, pvc_vclock:new()).

-spec start_transaction(coord(), non_neg_integer(), rvc()) -> {ok, tx()}.
start_transaction(Coord=#coordinator{self_ip=Ip, coordinator_id=LocalId}, Id, SVC) ->
    {ok, SVC} = start_internal(SVC, Coord),
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

%%====================================================================
%% Read Internal functions
%%====================================================================

start_internal(CVC, #coordinator{coordinator_id=Id, ring=Ring, sockets=Socks}) ->
    {P, N} = pvc_ring:random_indexnode(Ring),
    S = maps:get(N, Socks),
    ok = gen_tcp:send(S, <<Id:16, (ppb_grb_driver:start_tx(P, CVC))/binary>>),
    case gen_tcp:recv(S, 0) of
        {error, Reason} ->
            {error, Reason};
        {ok, <<Id:16, RawReply/binary>>} ->
            pvc_proto:decode_serv_reply(RawReply)
    end.

-spec op_internal(coord(), term(), rvc(), pvc_grb_rws:t()) -> {term(), pvc_grb_rws:t()}.
op_internal(Coord, Key, SVC, RWS) ->
    {Idx, NewVal, RedTS} = send_op_internal(Coord, Key, <<>>, SVC),
    {NewVal, pvc_grb_rws:put_ronly_op(Idx, Key, RedTS, RWS)}.

-spec op_internal(coord(), term(), term(), rvc(), pvc_grb_rws:t()) -> {term(), pvc_grb_rws:t()}.
op_internal(Coord, Key, Val, SVC, RWS) ->
    {Idx, NewVal, RedTS} = send_op_internal(Coord, Key, Val, SVC),
    {NewVal, pvc_grb_rws:put_op(Idx, Key, Val, RedTS, RWS)}.

-spec send_op_internal(coord(), term(), term(), rvc()) -> {index_node(), term(), non_neg_integer()}.
send_op_internal(#coordinator{ring=Ring, coordinator_id=Id, sockets=Socks}, Key, Val, SVC) ->
    Idx={P, N} = pvc_ring:get_key_indexnode(Ring, Key, ?GRB_BUCKET),
    S = maps:get(N, Socks),
    ok = gen_tcp:send(S, <<Id:16, (ppb_grb_driver:op_request(P, SVC, Key, Val))/binary>>),
    {ok, <<Id:16, RawReply/binary>>} = gen_tcp:recv(S, 0),
    {ok, NewVal, RedTS} = pvc_proto:decode_serv_reply(RawReply),
    {Idx, NewVal, RedTS}.

-spec commit_internal(coord(), tx()) -> rvc().
commit_internal(Coord, Tx) ->
    CVC = prepare_blue(Coord, Tx),
    ok = decide_blue(Coord, Tx, CVC),
    CVC.

-spec prepare_blue(coord(), tx()) -> rvc().
prepare_blue(#coordinator{sockets=Socks, coordinator_id=Id, replica_id=RId}, Tx) ->
    #transaction{rws=RWS, id=TxId, vc=SVC} = Tx,
    Sockets = pvc_grb_rws:fold(fun(Node, Partitions, SockAcc) ->
        S = maps:get(Node, Socks),
        Prepares = maps:fold(fun(Partition, {_, WS}, Acc) ->
            [{Partition, WS} | Acc]
        end, [], Partitions),
        Msg = ppb_grb_driver:prepare_blue_node(TxId, SVC, Prepares),
        ok = gen_tcp:send(S, <<Id:16, Msg/binary>>),
        [S | SockAcc]
    end, [], RWS),
    collect(Sockets, Id, RId, SVC).

-spec collect([inet:socket()], non_neg_integer(), term(), rvc()) -> rvc().
collect([], _, _, CVC) -> CVC;
collect([S | Rest], Id, ReplicaId, CVC) ->
    {ok, <<Id:16, RawReply/binary>>} = gen_tcp:recv(S, 0),
    Reply = pvc_proto:decode_serv_reply(RawReply),
    collect(Rest, Id, ReplicaId, update_vote(Reply, ReplicaId, CVC)).

-spec update_vote([{ok, partition_id(), non_neg_integer()}, ...], replica_id(), rvc()) -> rvc().
update_vote(Votes, ReplicaId, CVC) ->
    lists:foldl(fun({ok, _, PT}, Acc) ->
        pvc_vclock:set_time(ReplicaId,
                            erlang:max(PT, pvc_vclock:get_time(ReplicaId, Acc)),
                            Acc)
    end, CVC, Votes).

-spec decide_blue(coord(), tx(), rvc()) -> ok.
decide_blue(#coordinator{sockets=Socks, coordinator_id=Id}, Tx, CVC) ->
    #transaction{rws=RWS, id=TxId} = Tx,
    ok = pvc_grb_rws:fold(fun(Node, Partitions, _) ->
        S = maps:get(Node, Socks),
        DecidePartitions = maps:keys(Partitions),
        ok = gen_tcp:send(S, <<Id:16, (ppb_grb_driver:decide_blue_node(TxId, DecidePartitions, CVC))/binary>>)
    end, ok, RWS).
