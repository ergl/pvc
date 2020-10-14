-module(pvc_red_connection).
-behavior(gen_server).

-export([start_connection/3]).

-export([commit_red/5]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2]).

-define(CONN_OPTIONS, [binary,
                       {active, once},
                       {packet, 4},
                       {nodelay, true}]).

-record(state, {
    socket :: gen_tcp:socket(),
    id_len :: non_neg_integer(),
    msg_owners :: ets:tab()
}).

-record(handler, {
    conn_pid :: pid(),
    id_len :: non_neg_integer(),
    socket :: gen_tcp:socket(),
    msg_owners :: ets:tab()
}).

-type t() :: #handler{}.

-spec start_connection(inet:ip_address(), inet:port_number(), non_neg_integer()) -> {ok, t()} | {error, term()}.
start_connection(IP, Port, IdLen) ->
    case gen_server:start_link(?MODULE, [IP, Port, IdLen], []) of
        {ok, Pid} ->
            make_handler(Pid, IdLen);
        {error, {already_started, Pid}} ->
            make_handler(Pid, IdLen);
        Other ->
            {error, Other}
    end.

-spec make_handler(pid(), non_neg_integer()) -> {ok, t()}.
make_handler(Pid, IdLen) ->
    {ok, Socket} = gen_server:call(Pid, socket, infinity),
    {ok, Owners} = gen_server:call(Pid, owners_table, infinity),
    Handler = #handler{conn_pid=Pid, socket=Socket, msg_owners=Owners, id_len=IdLen},
    {ok, Handler}.

-spec commit_red(t(), non_neg_integer(), term(), term(), term()) -> ok.
commit_red(Handler, Id, TxId, SVC, Prepares) ->
    #handler{socket=Socket, msg_owners=Owners, id_len=IdLen} = Handler,
    Msg = <<Id:IdLen, (ppb_grb_driver:commit_red(TxId, SVC, Prepares))/binary>>,
    true = ets:insert_new(Owners, {Id, self()}),
    ok = gen_tcp:send(Socket, Msg),
    receive
        {?MODULE, Socket, Data} -> pvc_proto:decode_serv_reply(Data)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([IP, Port, IdLen]) ->
    case gen_tcp:connect(IP, Port, ?CONN_OPTIONS) of
        {error, Reason} ->
            {stop, Reason};
        {ok, Socket} ->
            Owners = ets:new(owners, [set, public, {write_concurrency, true}]),
            State = #state{socket=Socket,
                           id_len=IdLen,
                           msg_owners=Owners},
            {ok, State}
    end.

handle_call(owners_table, _From, S=#state{msg_owners=Owners}) ->
    {reply, {ok, Owners}, S};

handle_call(socket, _From, S=#state{socket=Socket}) ->
    {reply, {ok, Socket}, S};

handle_call(_E, _From, S) ->
    {reply, ok, S}.

handle_cast(_E, S) ->
    {noreply, S}.

handle_info({tcp, Socket, Data}, State=#state{socket=Socket, id_len=IdLen, msg_owners=Owners}) ->
    case Data of
        <<Id:IdLen, RawReply/binary>> ->
            case ets:take(Owners, Id) of
                [{Id, SenderPid}] ->
                    SenderPid ! {?MODULE, Socket, RawReply};
                [] ->
                    %% todo(borja): ???
                    ok
            end;
        _ ->
            ok
    end,
    ok = inet:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S) ->
    {stop, Reason, S};

handle_info(timeout, State) ->
    {stop, normal, State}.

terminate(_Reason, #state{socket=Socket}) ->
    ok = gen_tcp:close(Socket),
    ok.
