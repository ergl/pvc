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

-ifdef(TEST).
-export([prepare/2,
         decide/3]).
-endif.


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

-type transaction_ws() :: pvc_transaction_writeset:ws().

%% A key -> value cache for faster local reads
-type value_cache() :: orddict:orddict(term(), term()).

-type read_partitions() :: ordsets:ordset(partition_id()).

-type vc() :: pvc_vclock:vc(partition_id()).

-record(tx_state, {
    %% Identifier of the current transaction
    %% Must be unique among all other active transactions
    id :: transaction_id(),
    %% Marks if this transaction has been read-only so fat
    %% Read only transactions won't go through 2pc
    read_only = true :: boolean(),
    %% A local cache in sync with the writeset for faster local reads
    read_local_cache = orddict:new() :: value_cache(),
    %% Write set of the current transaction, partitioned
    %% by node and partition. This way, we can send
    %% to partitions only the subset they need to verify
    writeset = pvc_transaction_writeset:new() :: transaction_ws(),
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
update(State, Tx = #tx_state{read_local_cache=Cache, writeset=WS}, Key, Value) ->
    {NewCache, NewWS} = update_internal(State, Key, Value, Cache, WS),
    {ok, Tx#tx_state{read_only=false, writeset=NewWS, read_local_cache=NewCache}}.

%% @doc Update a batch of keys. Old values are replaced with the new ones.
-spec update(
    State :: coord_state(),
    Tx :: transaction(),
    Updates :: [{term(), term()}]
) -> {ok, transaction()}.

update(State, Tx = #tx_state{writeset=WS, read_local_cache=Cache}, Updates) when is_list(Updates) ->
    {NewCache, NewWS} = lists:foldl(fun({Key, Value}, {AccCache, AccWS}) ->
        update_internal(State, Key, Value, AccCache, AccWS)
    end, {Cache, WS}, Updates),
    {ok, Tx#tx_state{read_only=false, writeset=NewWS, read_local_cache=NewCache}}.

-spec commit(coord_state(), transaction()) -> ok | abort() | socket_error().
commit(_Conn, #tx_state{read_only=true}) -> ok;
commit(State, Tx) -> commit_internal(State, Tx).

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

    case key_updated(State, Key, Tx#tx_state.read_local_cache) of
        {true, Value} ->
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

    io:fwrite(standard_error, "{~p} ~p [~p]~n", [Tx#tx_state.id, ?FUNCTION_NAME, MsgId]),
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

-spec update_internal(coord_state(), term(), term(), value_cache(), transaction_ws()) -> {value_cache(), transaction_ws()}.
update_internal(State, Key, Value, Cache, WS) ->
    NewCache = orddict:store(Key, Value, Cache),
    IndexNode = pvc_ring:get_key_indexnode(State#coord_state.ring, Key),
    NewWS = pvc_transaction_writeset:put(IndexNode, Key, Value, WS),
    {NewCache, NewWS}.

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
                                            writeset=WS,
                                            vc_dep=CommitVC}) ->
    %% FIXME(borja): Getting read replies here, should generate a reference?
    Self = self(),
    OnReply = fun(_, Reply) -> Self ! {node_vote, Reply} end,
    pvc_transaction_writeset:fold(fun(Node, Partitions, ToACK) ->
        Connection = orddict:fetch(Node, Connections),
        Prepares = build_prepares(CommitVC, Partitions),
        PrepareMsg = ppb_protocol_driver:prepare_node(TxId, Prepares),
        io:fwrite(standard_error, "{~p} ~p [~p]~n", [TxId, ?FUNCTION_NAME, MsgId]),
        pvc_connection:send_async(Connection, MsgId, PrepareMsg, OnReply),
        ToACK + 1
    end, 0, WS).

%% @doc Build the individual prepare messages for each partition
build_prepares(CommitVC, Partitions) ->
    [{Partition, PWS, pvc_vclock:get_time(Partition, CommitVC)} || {Partition, PWS} <- Partitions].

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
            io:fwrite(standard_error, "Missed deadline for vote ~b. Retrying~n", [N]),
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

send_decide(Connections, MsgId, #tx_state{id=TxId, writeset=WS}, Outcome) ->
    pvc_transaction_writeset:fold(fun(Node, Partitions, ok) ->
        Connection = orddict:fetch(Node, Connections),
        %% No reply necessary
        [pvc_connection:send_cast(Connection, MsgId, encode_decide(P, TxId, Outcome))
            || {P, _} <- Partitions],
        ok
    end, ok, WS).

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

encode_decide(Partition, TxId, {error, _, _}) ->
    ppb_protocol_driver:decide_abort(Partition, TxId);

encode_decide(Partition, TxId, {ok, CommitVC}) ->
    ppb_protocol_driver:decide_commit(Partition, TxId, CommitVC).

-spec result({ok, vc()} | abort()) -> ok | abort().
result({error, _, Reason}) ->
    {abort, Reason};

result({ok, _}) ->
    ok.
