-module(pvc_shackle_transport).
-behavior(shackle_client).

-export([uniform_barrier/4,
         start_transaction/4,
         op_request/6,
         prepare_blue/5,
         decide_blue/5]).

%% API
-export([init/1,
         setup/2,
         handle_request/2,
         handle_data/2,
         handle_timeout/2,
         terminate/1]).

%% todo?
-record(state, {id_len :: non_neg_integer()}).

-spec uniform_barrier(atom(), non_neg_integer(), term(), term()) -> ok.
uniform_barrier(Pool, Id, Partition, CVC) ->
    shackle:call(Pool, {uniform_barrier, Id, Partition, CVC}, infinity).

-spec start_transaction(atom(), non_neg_integer(), term(), term()) -> {ok, term()}.
start_transaction(Pool, Id, Partition, CVC) ->
    shackle:call(Pool, {start_tx, Id, Partition, CVC}, infinity).

-spec op_request(atom, non_neg_integer(), term(), term(), term(), term()) -> {ok, term(), non_neg_integer()}.
op_request(Pool, Id, Partition, SVC, Key, Val) ->
    shackle:call(Pool, {op_request, Id, Partition, SVC, Key, Val}, infinity).

-spec prepare_blue(atom(), non_neg_integer(), term(), term(), [term()]) -> {ok, shackle:external_request_id()}.
prepare_blue(Pool, Id, TxId, SVC, Prepares) ->
    shackle:cast(Pool, {prepare_blue, Id, TxId, SVC, Prepares}, infinity).

-spec decide_blue(atom(), non_neg_integer(), term(), [term()], term()) -> ok.
decide_blue(Pool, Id, TxId, Partitions, CVC) ->
    shackle:call(Pool, {decide_blue, Id, TxId, Partitions, CVC}).

init(Options) ->
    {ok, #state{id_len = maps:get(id_len, Options, 16)}}.

setup(_Socket, State) ->
    {ok, State}.

handle_request({uniform_barrier, Id, Partition, CVC}, State=#state{id_len=IdLen}) ->
    {ok, Id, <<Id:IdLen, (ppb_grb_driver:uniform_barrier(Partition, CVC))/binary>>, State};

handle_request({start_tx, Id, Partition, CVC}, State=#state{id_len=IdLen}) ->
    {ok, Id, <<Id:IdLen, (ppb_grb_driver:start_tx(Partition, CVC))/binary>>, State};

handle_request({op_request, Id, Partition, SVC, Key, Val}, State=#state{id_len=IdLen}) ->
    {ok, Id, <<Id:IdLen, (ppb_grb_driver:op_request(Partition, SVC, Key, Val))/binary>>, State};

handle_request({prepare_blue, Id, TxId, SVC, Prepares}, State=#state{id_len=IdLen}) ->
    {ok, Id, <<Id:IdLen, (ppb_grb_driver:prepare_blue_node(TxId, SVC, Prepares))/binary>>, State};

handle_request({decide_blue, Id, TxId, Partitions, CVC}, State=#state{id_len=IdLen}) ->
    {ok, <<Id:IdLen, (ppb_grb_driver:decide_blue_node(TxId, Partitions, CVC))/binary>>, State};

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