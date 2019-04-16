-module(pvc).

-include("pvc.hrl").

%% Library lifetime management
-export([start/0,
         stop/0]).

%% API exports
-export([new/3,
         start_transaction/2,
         read/3,
         update/3,
         update/4,
         commit/2,
         close/1]).


-type cluster_conns() :: orddict:orddict(node_ip(),
                                         pvc_connection:connection()).

-record(coord_state, {
    %% The IP we're using to talk to the server
    %% Used to create a transaction id
    self_ip :: binary(),

    %% Routing info
    ring :: pvc_ring:ring(),

    %% Opened connection, one per node in the cluster
    connections :: cluster_conns(),

    %% An unique identifier for this coordinator instance
    %% This identifier will be used for all messages sent
    %% from this coordinator. At no point in the protocol
    %% do we send two messages without waiting for a reply,
    %% so there is no need to have more than one identifier
    %% per instance. Message ids are opaque to the server,
    %% so there is no possibility of conflict with other
    %% machines.
    instance_id :: non_neg_integer()
}).

%% An unique transaction id for this node ip,
%% supplied by the caller
-type transaction_id() :: {binary(), term()}.

-type partition_ws() :: pvc_writeset:ws(term(), term()).
-type ws() :: orddict:orddict(index_node(), partition_ws()).
-type read_partitions() :: ordsets:ordset(partition_id()).

-type vc() :: pvc_vclock:vc(partition_id()).

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
    writeset = orddict:new() :: ws(),
    %% VC representing the read versions
    vc_dep = pvc_vclock:new() :: vc(),
    %% VC representing the max threshold of version that should be read
    vc_aggr = pvc_vclock:new() :: vc(),
    %% What partitions have we read before
    read_partitions = ordsets:new() :: read_partitions()
}).

-opaque coord_state() :: #coord_state{}.
-opaque transaction() :: #tx_state{}.
-type abort() :: {abort, atom()}.

-export_type([coord_state/0,
              transaction_id/0,
              cluster_conns/0,
              transaction/0,
              abort/0,
              socket_error/0]).

-define(UNIMPL, erlang:error(not_implemented)).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Initalize the library.
%%
%%      Since PVC depends on pipesock to handle the connections,
%%      we need to start it beforehand.
%%
-spec start() -> ok | {error, Reason :: term()}.
start() ->
    application:ensure_started(pipesock).

-spec stop() -> ok | {error, Reason :: term()}.
stop() ->
    application:stop(pipesock).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Init the state of the coordinator
%%
%%      The coordinator needs the layout of the nodes in the cluster,
%%      and a list of {Ip, pvc_connection()} to each of those nodes.
%%
%%      Because we're using managed connections multiplexed
%%      across different instances, we need the caller to supply
%%      this extra state.
%%
-spec new(RingLayout :: pvc_ring:ring(),
          Connections :: cluster_conns(),
          Identifier :: non_neg_integer()) -> {ok, coord_state()}.

new(RingLayout, Connections, Identifier) ->
    [{_, Connection}| _] = Connections,
    %% All connections should have the same ip
    SelfIp = pvc_connection:get_local_ip(Connection),
    {ok, #coord_state{self_ip=SelfIp,
                      instance_id=Identifier,
                      ring=RingLayout,
                      connections=Connections}}.

%% @doc Start a new Transaction. Id should be unique for this node.
-spec start_transaction(coord_state(), term()) -> {ok, transaction()}.
start_transaction(#coord_state{self_ip=Ip}, Id) ->
    {ok, #tx_state{id={Ip, Id}}}.

%% @doc Read a key, or a list of keys.
%%
%%      If given a list of keys, will return a list of values,
%%      in the same order as the original keys.
%%
-spec read(coord_state(), transaction(), any()) -> {ok, any(), transaction()}
                                                | abort()
                                                | socket_error().

read(State, Tx, Keys) when is_list(Keys) ->
    catch read_batch(State, Keys, [], Tx);

read(State, Tx, Key) ->
    read_internal(Key, State, Tx).

%% @doc Update the given Key. Old value is replaced with new one
-spec update(coord_state(), transaction(), any(), any()) -> {ok, transaction()}.
update(State, Tx = #tx_state{writeset=WS}, Key, Value) ->
    NewWS = update_internal(State, Key, Value, WS),
    {ok, Tx#tx_state{read_only=false, writeset=NewWS}}.

%% @doc Update a batch of keys. Old values are replaced with the new ones.
-spec update(
    State :: coord_state(),
    Tx :: transaction(),
    Updates :: [{term(), term()}]
) -> {ok, transaction()}.

update(State, Tx = #tx_state{writeset=WS}, Updates) when is_list(Updates) ->
    NewWS = lists:foldl(fun({Key, Value}, AccWS) ->
        update_internal(State, Key, Value, AccWS)
    end, WS, Updates),
    {ok, Tx#tx_state{read_only=false, writeset=NewWS}}.

-spec commit(coord_state(), transaction()) -> ok | abort() | socket_error().
commit(_Conn, #tx_state{read_only=true}) ->
    ok;

commit(State, Tx) ->
    commit_internal(State, Tx).

-spec close(coord_state()) -> ok.
close(#coord_state{connections=Conns}) ->
    orddict:fold(fun(_Node, Connection, ok) ->
        pvc_connection:close(Connection)
    end, ok, Conns).

%%====================================================================
%% Read Internal functions
%%====================================================================

%% @doc Accumulatevly read the given keys.
%%
%%      On any error, return inmediately and stop reading further keys.
%%      Will not return any read value in that case.
%%
-spec read_batch(
    coord_state(),
    [term()],
    [term()],
    transaction()
) -> {ok, [term()], transaction()} | abort() | socket_error().

read_batch(_, [], ReadAcc, AccTx) ->
    {ok, lists:reverse(ReadAcc), AccTx};

read_batch(State, [Key | Rest], ReadAcc, AccTx) ->
    case read_internal(Key, State, AccTx) of
        {ok, Value, NewTx} ->
            read_batch(State, Rest, [Value | ReadAcc], NewTx);
        {abort, _}=Abort ->
            throw(Abort);
        {error, _}=Err ->
            throw(Err)
    end.

-spec read_internal(
    Key :: term(),
    State :: coord_state(),
    Tx :: transaction()
) -> {ok, term(), transaction()} | abort() | socket_error().

read_internal(Key, State=#coord_state{connections=Conns,
                                      instance_id=Unique}, Tx) ->

    case key_updated(State, Key, Tx#tx_state.writeset) of
        {ok, Value} ->
            {ok, Value, Tx};
        {false, {Partition, NodeIP}} ->
            Connection = orddict:fetch(NodeIP, Conns),
            remote_read(Unique, Partition, Connection, Key, Tx)
    end.

-spec remote_read(
    MsgId :: non_neg_integer(),
    Partition :: partition_id(),
    Connection :: pvc_connection:connection(),
    Key :: term(),
    Tx :: transaction()
) -> {ok, term(), transaction()} | abort() | socket_error().

remote_read(MsgId, Partition, Connection, Key, Tx) ->
    ReadRequest = ppb_protocol_driver:read_request(Partition,
                                                   Key,
                                                   Tx#tx_state.vc_aggr,
                                                   Tx#tx_state.read_partitions),

    case pvc_connection:send(Connection, MsgId, ReadRequest, 5000) of
        {error, Reason} ->
            {error, Reason};
        {ok, RawReply} ->
            case pvc_proto:decode_serv_reply(RawReply) of
                {error, Aborted} ->
                    {abort, Aborted};
                {ok, Value, VersionVC, MaxVC} ->
                    UpdatedTx = update_transacion(Partition,
                                                  VersionVC,
                                                  MaxVC,
                                                  Tx),
                    {ok, Value, UpdatedTx}
            end
    end.

%% @doc Update a transaction after a read.
-spec update_transacion(
    Partition :: partition_id(),
    VersionVC :: vc(),
    MaxVC :: vc(),
    Tx :: transaction()
) -> transaction().

update_transacion(Partition, VersionVC, MaxVC, Tx) ->
    #tx_state{vc_dep=VCdep, vc_aggr=VCaggr, read_partitions=HasRead} = Tx,
    Tx#tx_state{vc_dep = pvc_vclock:max(VersionVC, VCdep),
                vc_aggr = pvc_vclock:max(MaxVC, VCaggr),
                read_partitions = ordsets:add_element(Partition, HasRead)}.


%%====================================================================
%% Update Internal functions
%%====================================================================

%% @doc Check if a key has an assoc value in the given transaction writeset
%%
%%      Returns {ok, Value} if it was updated, or {false, Node} if not, where
%%      Node is the remote Antidote node we should route or read request
%%
-spec key_updated(
    State :: coord_state(),
    Key :: term(),
    WS :: ws()
) -> {ok, term()} | {false, index_node()}.

key_updated(State, Key, WS) ->
    Node = pvc_ring:get_key_indexnode(State#coord_state.ring, Key),
    NodeWS = get_node_writeset(Node, WS),
    pvc_writeset:get(Key, NodeWS, {false, Node}).

-spec get_node_writeset(index_node(), ws()) -> partition_ws().
get_node_writeset(Node, WS) ->
    case orddict:find(Node, WS) of
        error ->
            pvc_writeset:new();
        {ok, NodeWS} ->
            NodeWS
    end.

-spec update_internal(coord_state(), term(), term(), ws()) -> ws().
update_internal(State, Key, Value, WS) ->
    Node = pvc_ring:get_key_indexnode(State#coord_state.ring, Key),
    NewNodeWS = pvc_writeset:put(Key, Value, get_node_writeset(Node, WS)),
    orddict:store(Node, NewNodeWS, WS).

%%====================================================================
%% Commit Internal functions
%%====================================================================

-spec commit_internal(coord_state(),
                      transaction()) -> ok | abort() | socket_error().


commit_internal(#coord_state{connections=Connections, instance_id=Unique}, Tx) ->
    ok = send_prepare(Connections, Unique, Tx),
    case collect_votes(Connections, Tx) of
        {error, Reason} ->
            %% TODO(borja): Handle socket error on collect votes
            %% Socket error, should we flush Commit Queue on server?
            %% (send abort)
            {error, Reason};
        Outcome ->
            ok = send_decide(Connections, Unique, Tx, Outcome),
            result(Outcome)
    end.

-spec send_prepare(cluster_conns(), non_neg_integer(), transaction()) -> ok.
send_prepare(Connections, MsgId, #tx_state{id=TxId,
                                           writeset=WS,
                                           vc_dep=VersionVC}) ->

    Self = self(),
    orddict:fold(fun({Partition, NodeIP}, PartitionWS, ok) ->
        Connection = orddict:fetch(NodeIP, Connections),
        Version = pvc_vclock:get_time(Partition, VersionVC),
        PrepareMsg = ppb_protocol_driver:prepare(Partition,
                                                 TxId,
                                                 PartitionWS,
                                                 Version),

        ok = pvc_connection:send_async(Connection, MsgId, PrepareMsg, fun(ConnRef, Reply) ->
            Self ! {ConnRef, Reply}
        end)
    end, ok, WS).

%% FIXME(borja): Find better way to iterate through all sockets
%% It would be good to build some structure during the transaction
%% that makes the prepare/decide routine easier
-spec collect_votes(
    cluster_conns(),
    transaction()
) -> {ok, vc()} | abort() | socket_error().

collect_votes(Connections, #tx_state{writeset=WS, vc_dep=CommitVC}) ->
    orddict:fold(fun({_, NodeIP}, _, Acc) ->
        Connection = orddict:fetch(NodeIP, Connections),
        Ref = pvc_connection:get_ref(Connection),
        receive
            {Ref, RawReply} ->
                update_vote_acc(pvc_proto:decode_serv_reply(RawReply), Acc)
        end
    end, {ok, CommitVC}, WS).

-spec send_decide(Connections :: cluster_conns(),
                  MsgId :: non_neg_integer(),
                  transaction(), {ok, vc()} | abort()) -> ok.

send_decide(Connections, MsgId, #tx_state{id=TxId, writeset=WS}, Outcome) ->
    orddict:fold(fun({Partition, NodeIP}, _, ok) ->
        Connection = orddict:fetch(NodeIP, Connections),
        DecideMsg = encode_decide(Partition, TxId, Outcome),
        %% No reply necessary
        ok = pvc_connection:send_cast(Connection, MsgId, DecideMsg)
    end, ok, WS).

%% Vote / Acc can be
%% either {error, Reason} or {error, Partition, Reason}
%% The first one is socket error, the second is a negative Vote
update_vote_acc(_, Acc) when element(1, Acc) =:= error ->
    Acc;

update_vote_acc(Vote, _) when element(1, Vote) =:= error ->
    Vote;

update_vote_acc({ok, Partition, Seq}, {ok, CommitVC}) ->
    {ok, pvc_vclock:set_time(Partition, Seq, CommitVC)}.

encode_decide(Partition, TxId, {error, _, _}) ->
    ppb_protocol_driver:decide_abort(Partition, TxId);

encode_decide(Partition, TxId, {ok, CommitVC}) ->
    ppb_protocol_driver:decide_commit(Partition, TxId, CommitVC).

-spec result({ok, vc()} | abort()) -> ok | abort().
result({error, _, Reason}) ->
    {abort, Reason};

result({ok, _}) ->
    ok.
