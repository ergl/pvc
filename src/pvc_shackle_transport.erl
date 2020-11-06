-module(pvc_shackle_transport).
-behavior(shackle_client).

-export([uniform_barrier/4,
         start_transaction/4,
         start_read/5,
         get_key_version/5,
         get_key_version_again/5,
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

-spec start_read(atom(), non_neg_integer(), term(), term(), binary()) -> {ok, binary(), term()}.
start_read(Pool, Id, Partition, CVC, Key) ->
    shackle:call(Pool, {start_read, Id, Partition, CVC, Key}, infinity).

-spec get_key_version(atom(), non_neg_integer(), term(), term(), binary) -> {ok, binary()}.
get_key_version(Pool, Id, Partition, SVC, Key) ->
    shackle:call(Pool, {get_key_vsn, Id, Partition, SVC, Key}, infinity).

-spec get_key_version_again(atom(), non_neg_integer(), term(), term(), binary) -> {ok, binary()}.
get_key_version_again(Pool, Id, Partition, SVC, Key) ->
    shackle:call(Pool, {get_key_vsn_again, Id, Partition, SVC, Key}, infinity).

-spec prepare_blue(atom(), non_neg_integer(), term(), term(), [term()]) -> {ok, shackle:external_request_id()}.
prepare_blue(Pool, Id, TxId, SVC, Prepares) ->
    shackle:cast(Pool, {prepare_blue, Id, TxId, SVC, Prepares}, self(), infinity).

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

handle_request({start_read, Id, Partition, CVC, Key}, State=#state{id_len=IdLen}) ->
    {ok, Id, <<Id:IdLen, (ppb_grb_driver:start_read(Partition, CVC, Key))/binary>>, State};

handle_request({get_key_vsn, Id, Partition, SVC, Key}, State=#state{id_len=IdLen}) ->
    {ok, Id, <<Id:IdLen, (ppb_grb_driver:key_version(Partition, SVC, Key))/binary>>, State};

handle_request({get_key_vsn_again, Id, Partition, SVC, Key}, State=#state{id_len=IdLen}) ->
    {ok, Id, <<Id:IdLen, (ppb_grb_driver:key_version_again(Partition, SVC, Key))/binary>>, State};

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
