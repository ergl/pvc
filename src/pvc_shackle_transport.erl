-module(pvc_shackle_transport).
-behavior(shackle_client).

-export([uniform_barrier/3,
         start_transaction/3,
         read_request/7,
         cast_read_request/7,
         cast_read_operation/7,
         update_request/7,
         cast_update_request/7,
         cast_partition_read_request/6,
         cast_partition_update_request/6,
         prepare_blue/4,
         decide_blue/4]).

%% Simple update API
-export([cast_update_send/5]).

%% API
-export([init/1,
         setup/2,
         handle_request/2,
         handle_data/2,
         handle_timeout/2,
         terminate/1]).

-record(state, {
    id_len :: non_neg_integer(),
    req_counter :: non_neg_integer(),
    max_req_id :: non_neg_integer()
}).
-type state() :: #state{}.

-spec uniform_barrier(atom(), term(), term()) -> ok.
uniform_barrier(Pool, Partition, CVC) ->
    shackle:call(Pool, {uniform_barrier, Partition, CVC}, infinity).

-spec start_transaction(atom(), term(), term()) -> {ok, term()}.
start_transaction(Pool, Partition, CVC) ->
    shackle:call(Pool, {start_tx, Partition, CVC}, infinity).

-spec read_request(atom(), term(), term(), term(), term(), term(), boolean()) -> {ok, term()}.
read_request(Pool, Partition, TxId, SVC, Key, Type, ReadAgain) ->
    shackle:call(Pool, {read_request, Partition, TxId, SVC, Key, Type, ReadAgain}, infinity).

-spec cast_read_request(atom(), term(), term(), term(), term(), term(), boolean()) -> {ok, shackle:external_request_id()}.
cast_read_request(Pool, Partition, TxId, SVC, Key, Type, ReadAgain) ->
    shackle:cast(Pool, {read_request, Partition, TxId, SVC, Key, Type, ReadAgain}, self(), infinity).

-spec cast_read_operation(atom(), term(), term(), term(), term(), term(), boolean()) -> {ok, shackle:external_request_id()}.
cast_read_operation(Pool, Partition, TxId, SVC, Key, ReadOp, ReadAgain) ->
    shackle:cast(Pool, {read_operation, Partition, TxId, SVC, Key, ReadOp, ReadAgain}, self(), infinity).

-spec update_request(atom(), term(), term(), term(), term(), term(), boolean()) -> {ok, term()}.
update_request(Pool, Partition, TxId, SVC, Key, Operation, ReadAgain) ->
    shackle:call(Pool, {update_request, Partition, TxId, SVC, Key, Operation, ReadAgain}, infinity).

-spec cast_update_request(atom(), term(), term(), term(), term(), term(), boolean()) -> {ok, shackle:external_request_id()}.
cast_update_request(Pool, Partition, TxId, SVC, Key, Operation, ReadAgain) ->
    shackle:cast(Pool, {update_request, Partition, TxId, SVC, Key, Operation, ReadAgain}, self(), infinity).

-spec cast_partition_read_request(atom(), non_neg_integer(), term(), term(), boolean(), [{term(), term()}]) -> {ok, shackle:external_request_id()}.
cast_partition_read_request(Pool, Partition, TxId, SVC, ReadAgain, KeyTypes) ->
    shackle:cast(Pool, {read_partition, Partition, TxId, SVC, ReadAgain, KeyTypes}, self(), infinity).

-spec cast_partition_update_request(atom(), non_neg_integer(), term(), term(), boolean(), [{term(), term()}]) -> {ok, shackle:external_request_id()}.
cast_partition_update_request(Pool, Partition, TxId, SVC, ReadAgain, KeyOps) ->
    shackle:cast(Pool, {update_partition, Partition, TxId, SVC, ReadAgain, KeyOps}, self(), infinity).

-spec cast_update_send(atom(), non_neg_integer(), term(), term(), term()) -> {ok, shackle:external_request_id()}.
cast_update_send(Pool, Partition, TxId, Key, Operation) ->
    shackle:cast(Pool, {update_send, Partition, TxId, Key, Operation}, self(), infinity).

-spec prepare_blue(atom(), term(), term(), [non_neg_integer()]) -> {ok, shackle:external_request_id()}.
prepare_blue(Pool, TxId, SVC, Partitions) ->
    shackle:cast(Pool, {prepare_blue, TxId, SVC, Partitions}, self(), infinity).

-spec decide_blue(atom(), term(), [term()], term()) -> ok.
decide_blue(Pool, TxId, Partitions, CVC) ->
    shackle:call(Pool, {decide_blue, TxId, Partitions, CVC}).

init(Options) ->
    IdLen = maps:get(id_len, Options, 16),
    MaxId = trunc(math:pow(2, IdLen)),
    {ok, #state{id_len=IdLen,
                req_counter=0,
                max_req_id=MaxId}}.

setup(_Socket, State) ->
    {ok, State}.

handle_request({uniform_barrier, Partition, CVC}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, Req, <<Req:Len, (ppb_grb_driver:uniform_barrier(Partition, CVC))/binary>>, incr_req(S)};

handle_request({start_tx, Partition, CVC}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, Req, <<Req:Len, (ppb_grb_driver:start_tx(Partition, CVC))/binary>>, incr_req(S)};

handle_request({read_request, Partition, TxId, SVC, Key, Type, ReadAgain}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, Req, <<Req:Len, (ppb_grb_driver:read_request(Partition, TxId, SVC, ReadAgain, Key, Type))/binary>>, incr_req(S)};

handle_request({read_operation, Partition, TxId, SVC, Key, ReadOp, ReadAgain}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, Req, <<Req:Len, (ppb_grb_driver:read_operation_request(Partition, TxId, SVC, ReadAgain, Key, ReadOp))/binary>>, incr_req(S)};

handle_request({update_request, Partition, TxId, SVC, Key, Operation, ReadAgain}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, Req, <<Req:Len, (ppb_grb_driver:update_request(Partition, TxId, SVC, ReadAgain, Key, Operation))/binary>>, incr_req(S)};

handle_request({read_partition, Partition, TxId, SVC, ReadAgain, KeyTypes}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, Req, <<Req:Len, (ppb_grb_driver:read_request_partition(Partition, TxId, SVC, ReadAgain, KeyTypes))/binary>>, incr_req(S)};

handle_request({update_partition, Partition, TxId, SVC, ReadAgain, KeyOps}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, Req, <<Req:Len, (ppb_grb_driver:update_request_partition(Partition, TxId, SVC, ReadAgain, KeyOps))/binary>>, incr_req(S)};

handle_request({update_send, Partition, TxId, Key, Operation}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, Req, <<Req:Len, (ppb_grb_driver:op_send(Partition, TxId, Key, Operation))/binary>>, incr_req(S)};

handle_request({prepare_blue, TxId, SVC, Partitions}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, Req, <<Req:Len, (ppb_grb_driver:prepare_blue_node(TxId, SVC, Partitions))/binary>>, incr_req(S)};

handle_request({decide_blue, TxId, Partitions, CVC}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, <<Req:Len, (ppb_grb_driver:decide_blue_node(TxId, Partitions, CVC))/binary>>, incr_req(S)};

handle_request(_Request, _State) ->
    erlang:error(unknown_request).

handle_data(Data, State=#state{id_len=IdLen}) ->
    case Data of
        <<Id:IdLen, RawReply/binary>> ->
            {ok, [{Id, pvc_proto:decode_serv_reply(RawReply)}], State};
        _ ->
            {error, bad_data, State}
    end.

handle_timeout(RequestId, State) ->
    {ok, {RequestId, {error, timeout}}, State}.

terminate(_State) -> ok.

%% Util

-spec incr_req(state()) -> state().
incr_req(S=#state{req_counter=ReqC, max_req_id=MaxId}) ->
    S#state{req_counter=((ReqC + 1) rem MaxId)}.
