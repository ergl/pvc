-module(pvc_red_connection).
-behavior(shackle_client).
-include("pvc.hrl").

-export([commit_red/6]).

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

-spec commit_red(Pool :: atom(),
                 Partition :: partition_id(),
                 TxId :: term(),
                 Label :: term(),
                 SVC :: #{replica_id() => non_neg_integer()},
                 Prepres :: [{partition_id(), [], #{}}]) -> {ok, #{}} | {abort, term()}.

commit_red(Pool, Partition, TxId, Label, SVC, Prepares) ->
    shackle:call(Pool, {commit_red, Partition, TxId, Label, SVC, Prepares}, infinity).

%%%===================================================================
%%% shackle callbacks
%%%===================================================================

init(Options) ->
    IdLen = maps:get(id_len, Options, 16),
    MaxId = trunc(math:pow(2, IdLen)),
    {ok, #state{id_len=IdLen,
                req_counter=0,
                max_req_id=MaxId}}.

setup(_Socket, State) ->
    {ok, State}.

handle_request({commit_red, Partition, TxId, Label, SVC, Prepares}, S=#state{req_counter=Req, id_len=Len}) ->
    {ok, Req, <<Req:Len, (ppb_grb_driver:commit_red(Partition, TxId, Label, SVC, Prepares))/binary>>, incr_req(S)};

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
