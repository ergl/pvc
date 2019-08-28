-module(pvc).

-include("pvc.hrl").

%% Library lifetime management
-export([start/0,
         stop/0]).

%% API exports
-export([new/3,
         new/4,
         start_transaction/2,
         read/3,
         update/3,
         update/4,
         commit/2,
         close/1]).

-ifdef(TEST).
-export([prepare/2,
         decide/3]).
-endif.


-type cluster_conns() :: orddict:orddict(node_ip(),
                                         pvc_connection:connection()).


-type protocol() :: psi | ser.
-define(DEFAULT_PROTOCOL, psi).

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
    instance_id :: non_neg_integer(),

    %% Identifies the version of the protocol used by this
    %% coordinator. A coordinator can only spawn transactions
    %% of a single type.
    protocol_version = ?DEFAULT_PROTOCOL :: protocol()
}).

%% An unique transaction id for this node ip,
%% supplied by the caller
-type transaction_id() :: {binary(), term()}.

-type transaction_rws() :: pvc_ser_transaction_readwriteset:rws().
-type transaction_ws() :: pvc_transaction_writeset:ws().

%% A key -> value cache for faster local reads
-type value_cache() :: orddict:orddict(term(), term()).

-type read_partitions() :: ordsets:ordset(partition_id()).

-type vc() :: pvc_vclock:vc(partition_id()).

%% Transaction-dependent state for the PVC protocol
-record(psi_state, {
    %% Marks if this transaction has been read-only so far
    %% Read only transactions won't go through 2pc
    read_only = true :: boolean(),
    %% Write set of the current transaction, partitioned
    %% by node and partition. This way, we can send
    %% to partitions only the subset they need to verify
    writeset = pvc_transaction_writeset:new() :: transaction_ws()
}).

%% Transaction-dependent state for Serializability
-record(ser_state, {
    %% Read/Write set of the current transaction, partitioned
    %% by node and partition. This way, we can send
    %% to partitions only the subset they need to verify
    read_write_set = pvc_ser_transaction_readwriteset:new() :: transaction_rws()
}).

-type protocol_state() :: #psi_state{} | #ser_state{}.
-record(tx_state, {
    %% Identifier of the current transaction
    %% Must be unique among all other active transactions
    id :: transaction_id(),
    %% A local cache in sync with the writeset for faster local reads
    read_local_cache = orddict:new() :: value_cache(),
    %% VC representing the read versions
    vc_dep = pvc_vclock:new() :: vc(),
    %% VC representing the max threshold of version that should be read
    vc_aggr = pvc_vclock:new() :: vc(),
    %% What partitions have we read before
    read_partitions = ordsets:new() :: read_partitions(),
    protocol_state :: protocol_state()
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

-spec new(RingLayout :: pvc_ring:ring(),
          Connections :: cluster_conns(),
          Identifier :: non_neg_integer()) -> {ok, coord_state()}.

new(RingLayout, Connections, Identifier) ->
    new(RingLayout, Connections, Identifier, ?DEFAULT_PROTOCOL).

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
          Identifier :: non_neg_integer(),
          Protocol :: protocol()) -> {ok, coord_state()}.

new(RingLayout, Connections, Identifier, Protocol) ->
    [{_, Connection}| _] = Connections,
    %% All connections should have the same ip
    SelfIp = pvc_connection:get_local_ip(Connection),
    {ok, #coord_state{self_ip=SelfIp,
                      instance_id=Identifier,
                      ring=RingLayout,
                      connections=Connections,
                      protocol_version=Protocol}}.

%% @doc Start a new Transaction. Id should be unique for this node.
-spec start_transaction(coord_state(), term()) -> {ok, transaction()}.
start_transaction(#coord_state{self_ip=Ip, protocol_version=Prot}, Id) ->
    {ok, #tx_state{id={Ip, Id}, protocol_state=protocol_state(Prot)}}.

%% @doc Read a key, or a list of keys.
%%
%%      If given a list of keys, will return a list of values,
%%      in the same order as the original keys.
%%
-spec read(coord_state(), transaction(), any()) -> {ok, any(), transaction()}
                                                 | abort().

read(State, Tx, Keys) when is_list(Keys) ->
    catch read_batch(State, Keys, [], Tx);

read(State, Tx, Key) ->
    read_internal(Key, State, Tx).

%% @doc Update the given Key. Old value is replaced with new one
-spec update(coord_state(), transaction(), any(), any()) -> {ok, transaction()}.
update(State, Tx = #tx_state{read_local_cache=Cache, protocol_state=ProtState}, Key, Value) ->
    {NewCache, NewProtState} = update_internal(State, Key, Value, Cache, ProtState),
    {ok, Tx#tx_state{read_local_cache=NewCache, protocol_state=NewProtState}}.

%% @doc Update a batch of keys. Old values are replaced with the new ones.
-spec update(State :: coord_state(),
             Tx :: transaction(),
             Updates :: [{term(), term()}]) -> {ok, transaction()}.

update(State, Tx = #tx_state{read_local_cache=Cache, protocol_state=ProtState}, Updates) when is_list(Updates) ->
    {NewCache, NewProtState} = lists:foldl(fun({Key, Value}, {AccCache, AccProtState}) ->
        update_internal(State, Key, Value, AccCache, AccProtState)
    end, {Cache, ProtState}, Updates),
    {ok, Tx#tx_state{read_local_cache=NewCache, protocol_state=NewProtState}}.

-spec commit(coord_state(), transaction()) -> ok | abort() | socket_error().
%% Read-only transactions don't commit in the PSI protocol
commit(_Conn, #tx_state{protocol_state=#psi_state{read_only=true}}) -> ok;
commit(State, Tx) -> commit_internal(State, Tx).

-spec close(coord_state()) -> ok.
close(#coord_state{connections=Conns}) ->
    orddict:fold(fun(_Node, Connection, ok) ->
        pvc_connection:close(Connection)
    end, ok, Conns).

%%====================================================================
%% Start Internal functions
%%====================================================================

-spec protocol_state(protocol()) -> protocol_state().
protocol_state(psi) -> #psi_state{};
protocol_state(ser) -> #ser_state{}.

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
) -> {ok, [term()], transaction()} | abort().

read_batch(_, [], ReadAcc, AccTx) ->
    {ok, lists:reverse(ReadAcc), AccTx};

read_batch(State, [Key | Rest], ReadAcc, AccTx) ->
    case read_internal(Key, State, AccTx) of
        {ok, Value, NewTx} ->
            read_batch(State, Rest, [Value | ReadAcc], NewTx);
        {abort, _}=Abort ->
            throw(Abort)
    end.

-spec read_internal(
    Key :: term(),
    State :: coord_state(),
    Tx :: transaction()
) -> {ok, term(), transaction()} | abort().

read_internal(Key, State=#coord_state{connections=Conns,
                                      instance_id=Unique}, Tx) ->

    case key_updated(State, Key, Tx#tx_state.read_local_cache) of
        {true, Value} ->
            {ok, Value, Tx};
        {false, {_, NodeIP}=IndexNode} ->
            Connection = orddict:fetch(NodeIP, Conns),
            remote_read(Unique, IndexNode, Connection, Key, Tx)
    end.

-spec remote_read(
    MsgId :: non_neg_integer(),
    IndexNode :: index_node(),
    Connection :: pvc_connection:connection(),
    Key :: term(),
    Tx :: transaction()
) -> {ok, term(), transaction()} | abort().

remote_read(MsgId, {Partition, _}=IndexNode, Connection, Key, Tx) ->
    ReadRequest = ppb_protocol_driver:read_request(Partition,
                                                   Key,
                                                   Tx#tx_state.vc_aggr,
                                                   Tx#tx_state.read_partitions),

    {ok, RawReply} = pvc_connection:send(Connection, MsgId, ReadRequest),
    case pvc_proto:decode_serv_reply(RawReply) of
        {error, Aborted} ->
            {abort, Aborted};
        {ok, Value, VersionVC, MaxVC} ->
            UpdatedTx = update_transacion(IndexNode,
                                          Key,
                                          VersionVC,
                                          MaxVC,
                                          Tx),
            {ok, Value, UpdatedTx}
    end.

%% @doc Update a transaction after a read.
-spec update_transacion(
    IndexNode :: index_node(),
    Key :: term(),
    VersionVC :: vc(),
    MaxVC :: vc(),
    Tx :: transaction()
) -> transaction().

update_transacion({Partition, _}=IndexNode, Key, VersionVC, MaxVC, Tx) ->
    #tx_state{vc_dep=VCdep,
              vc_aggr=VCaggr,
              read_partitions=HasRead,
              protocol_state=ProtState} = Tx,

    Tx#tx_state{vc_dep = pvc_vclock:max(VersionVC, VCdep),
                vc_aggr = pvc_vclock:max(MaxVC, VCaggr),
                read_partitions = ordsets:add_element(Partition, HasRead),
                protocol_state = read_protocol_state(IndexNode, Key, VersionVC, ProtState)}.

%% @doc For serializability, update the transaction readset
%%
%%      The rest of the protocols don't change
%%
-spec read_protocol_state(IndexNode :: index_node(),
                          Key :: term(),
                          VersionVC :: vc(),
                          ST :: protocol_state()) -> protocol_state().

read_protocol_state({Partition, _}=IndexNode, Key, VersionVC, ST=#ser_state{read_write_set=RWS}) ->
    Version = pvc_vclock:get_time(Partition, VersionVC),
    ST#ser_state{read_write_set = pvc_ser_transaction_readwriteset:put_rs(IndexNode, Key, Version, RWS)};

read_protocol_state(_, _, _, State) ->
    State.

%%====================================================================
%% Update Internal functions
%%====================================================================

%% @doc Check if a key has an assoc value in the given transaction writeset
%%
%%      Returns {true, Value} if it was updated, or {false, Node} if not, where
%%      Node is the remote Antidote node we should route or read request
%%
-spec key_updated(
    State :: coord_state(),
    Key :: term(),
    Cache :: value_cache()
) -> {true, term()} | {false, index_node()}.

key_updated(State, Key, Cache) ->
    case orddict:find(Key, Cache) of
        {ok, Value} ->
            {true, Value};
        error ->
            Node = pvc_ring:get_key_indexnode(State#coord_state.ring, Key),
            {false, Node}
    end.

-spec update_internal(State :: coord_state(),
                      Key :: term(),
                      Value :: term(),
                      Cache :: value_cache(),
                      ProtocolState :: protocol_state()) -> {value_cache(), protocol_state()}.

update_internal(State, Key, Value, Cache, ProtState) ->
    NewCache = orddict:store(Key, Value, Cache),
    IndexNode = pvc_ring:get_key_indexnode(State#coord_state.ring, Key),
    {NewCache, write_protocol_state(IndexNode, Key, Value, ProtState)}.

write_protocol_state(IndexNode, Key, Value, ST=#psi_state{writeset=WS}) ->
    ST#psi_state{read_only = false,
                 writeset = pvc_transaction_writeset:put(IndexNode, Key, Value, WS)};

write_protocol_state(IndexNode, Key, Value, ST=#ser_state{read_write_set=RWS}) ->
    ST#ser_state{read_write_set = pvc_ser_transaction_readwriteset:put_ws(IndexNode,
                                                                          Key,
                                                                          Value,
                                                                          RWS)}.

%%====================================================================
%% Commit Internal functions
%%====================================================================

-spec commit_internal(coord_state(),
                      transaction()) -> ok | abort() | socket_error().

commit_internal(State, Tx) ->
    decide(State, Tx, prepare(State, Tx)).

-spec prepare(coord_state(), transaction()) -> {ok, vc()} | abort() | socket_error().
prepare(#coord_state{connections=Connections, instance_id=Unique}, Tx) ->
    ToAck = send_prepares(Connections, Unique, Tx),
    collect_votes(ToAck, {ok, Tx#tx_state.vc_dep}).

%% @doc Send all the prepare messages in parallel
%%
%%      Replies with the number of partitions we need to ack
%%
-spec send_prepares(cluster_conns(), non_neg_integer(), transaction()) -> non_neg_integer().
send_prepares(Connections, MsgId, #tx_state{id=TxId,
                                            vc_dep=CommitVC,
                                            protocol_state=ProtState}) ->
    Self = self(),
    OnReply = fun(_, Reply) -> Self ! {node_vote, Reply} end,
    ForEach = fun(Protocol, Node, Partitions) ->
        Connection = orddict:fetch(Node, Connections),
        Prepares = build_prepares(CommitVC, Partitions),
        PrepareMsg = ppb_protocol_driver:prepare_node(TxId, Protocol, Prepares),
        pvc_connection:send_async(Connection, MsgId, PrepareMsg, OnReply)
    end,

    for_each_node_count(ProtState, ForEach).

%% @doc Build the individual prepare messages for each partition
-spec build_prepares(
    CommitVC :: vc(),
    Partitions :: pvc_transaction_writeset:partitions_writeset()
                | pvc_ser_transaction_readwriteset:partitions_readwriteset()
) -> [{partition_id(), #{term() => term()}, non_neg_integer()}, ...].

build_prepares(CommitVC, Partitions) ->
    maps:fold(fun(Partition, Data, Acc) ->
        [{Partition, Data, pvc_vclock:get_time(Partition, CommitVC)} | Acc]
    end, [], Partitions).

%% @doc Collect all the votes by all the partitions
%%
%%      We collect all even if the first one is an abort, as we don't
%%      want the process to have messages in the queue.
%%
-spec collect_votes(non_neg_integer(),
                    {ok, vc()} | abort() | socket_error()) -> {ok, vc()} | abort() | socket_error().
collect_votes(0, VoteAcc) ->
    VoteAcc;
collect_votes(N, VoteAcc) ->
    receive
        {node_vote, VoteReply} ->
            Reply = pvc_proto:decode_serv_reply(VoteReply),
            collect_votes(N - 1, update_vote_acc(Reply, VoteAcc))
        after 5000 ->
            collect_votes(N, VoteAcc)
    end.

-spec decide(coord_state(),
             transaction(),
             {ok, vc()} | abort() | socket_error()) -> ok | abort() | socket_error().

%% TODO(borja): Handle socket error on collect votes
%% Socket error, should we flush Commit Queue on server?
%% (send abort)
decide(_, _, {error, Reason}) -> {error, Reason};
decide(#coord_state{connections=Connections, instance_id=Unique}, Tx, Outcome) ->
    ok = send_decide(Connections, Unique, Tx, Outcome),
    result(Outcome).

-spec send_decide(Connections :: cluster_conns(),
                  MsgId :: non_neg_integer(),
                  transaction(), {ok, vc()} | abort()) -> ok.

send_decide(Connections, MsgId, #tx_state{id=TxId, protocol_state=ProtState}, Outcome) ->
    ForEach = fun(Protocol, Node, Partitions) ->
        Connection = orddict:fetch(Node, Connections),
        %% No reply necessary
        [pvc_connection:send_cast(Connection, MsgId, encode_decide(P, TxId, Protocol, Outcome))
            || P <- maps:keys(Partitions)]
    end,

    for_each_node(ProtState, ForEach).

%% @doc Update prepare accumulator with votes from a node
update_vote_acc(Votes, Acc) ->
    lists:foldl(fun update_vote_acc_internal/2, Acc, Votes).

%% Vote / Acc can be
%% either {error, Reason} or {error, Partition, Reason}
%% The first one is socket error, the second is a negative Vote
update_vote_acc_internal(_, Acc) when element(1, Acc) =:= error ->
    Acc;

update_vote_acc_internal(Vote, _) when element(1, Vote) =:= error ->
    Vote;

update_vote_acc_internal({ok, Partition, Seq}, {ok, CommitVC}) ->
    {ok, pvc_vclock:set_time(Partition, Seq, CommitVC)}.

encode_decide(Partition, TxId, Protocol, {error, _, _}) ->
    ppb_protocol_driver:decide_abort(Partition, TxId, Protocol);

encode_decide(Partition, TxId, Protocol, {ok, CommitVC}) ->
    ppb_protocol_driver:decide_commit(Partition, TxId, Protocol, CommitVC).

%% FIXME(borja): Types
%% abort() is defined as {abort, _}
%% Does this function ever get a negative vote?
-spec result({ok, vc()} | abort()) -> ok | abort().
result({error, _, Reason}) ->
    {abort, Reason};

result({ok, _}) ->
    ok.

%%====================================================================
%% Generic Util functions
%%====================================================================

-spec for_each_node_count(protocol_state(), fun((...) -> ok)) -> non_neg_integer().
for_each_node_count(ProtState, Fun) ->
    node_fold(ProtState, Fun, 0, fun(Acc) -> Acc + 1 end).

-spec for_each_node(protocol_state(), fun((...) -> ok)) -> ok.
for_each_node(ProtState, Fun) ->
    node_fold(ProtState, Fun, ok, fun(_) -> ok end).

-spec node_fold(protocol_state(),
                DoFun :: fun((...) -> ok),
                Init :: term(),
                AccFun :: fun((term()) -> term())) -> term().

node_fold(#psi_state{writeset=WS}, DoFun, Init, AccFun) ->
    pvc_transaction_writeset:fold(fun(Node, Partitions, Acc) ->
        _ = DoFun(psi, Node, Partitions),
        AccFun(Acc)
    end, Init, WS);

node_fold(#ser_state{read_write_set=RWS}, DoFun, Init, AccFun) ->
    pvc_ser_transaction_readwriteset:fold(fun(Node, Partitions, Acc) ->
        _ = DoFun(ser, Node, Partitions),
        AccFun(Acc)
    end, Init, RWS).
