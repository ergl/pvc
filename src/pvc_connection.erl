-module(pvc_connection).

-include("pvc.hrl").

%% API
-export([new/2,
         get_ref/1,
         get_local_ip/1,
         send/4,
         send_async/4,
         send_cast/3,
         close/1]).

-type connection() :: pipesock_conn:conn_handle().
-export_type([connection/0]).

-define(VALID_SEND_SYNC(A, B), is_integer(A) andalso is_binary(B)).
-define(VALID_SEND_ASYNC(A, B), is_function(A, 2) andalso is_binary(B)).

-spec new(Ip :: node_ip(),
          Port :: inet:port_number()) -> {ok, connection()} | {error, term()}.

new(Ip, Port) ->
    pipesock_conn:open(Ip, Port, #{id_len => 16}).

%% @doc Get the unique reference of the connection.
%%
%%      The connection will reply with this tag to the callback
%%      supplied by send/3
-spec get_ref(connection()) -> reference().
get_ref(Handle) ->
    pipesock_conn:get_ref(Handle).

-spec get_local_ip(connection()) -> binary().
get_local_ip(Handle) ->
    Ip = pipesock_conn:get_self_ip(Handle),
    list_to_binary(inet:ntoa(Ip)).

%% @doc Send a message
%%
%%      This will do the msg id wrap/unwrap for us
%%
-spec send(Handle :: connection(),
           MsgId :: non_neg_integer(),
           Msg :: binary(),
           Timeout :: non_neg_integer()) -> {ok, term()} | {error, timeout}.

send(Handle, MsgId, Msg, Timeout) when ?VALID_SEND_SYNC(Timeout, Msg) ->
    case pipesock_conn:send_sync(Handle, wrap_msg(Handle, MsgId, Msg), Timeout) of
        {error, timeout} ->
            {error, timeout};
        {ok, Reply} ->
            {ok, unwrap_msg(Handle, Reply)}
    end.

%% @doc Send a message asynchronously, execute given callback on reply
-spec send_async(Handle :: connection(),
                 MsgId :: non_neg_integer(),
                 Msg :: binary(),
                 Callback :: fun((reference(), binary()) -> ok)) -> ok.

send_async(Handle, MsgId, Msg, Callback) when ?VALID_SEND_ASYNC(Callback, Msg) ->
    %% Wrap/unwrap the message before executing the caller callback
    WrapCallback = fun(ConnRef, Reply) ->
        Callback(ConnRef, unwrap_msg(Handle, Reply))
    end,
    pipesock_conn:send_cb(Handle, wrap_msg(Handle, MsgId, Msg), WrapCallback).

%% @doc Send a message and forget
-spec send_cast(Handle :: connection(), MsgId :: non_neg_integer(), Msg :: binary()) -> ok.
send_cast(Handle, MsgId, Msg) ->
    pipesock_conn:send_and_forget(Handle, wrap_msg(Handle, MsgId, Msg)).

-spec close(connection()) -> ok.
close(Handle) ->
    pipesock_conn:close(Handle).

%% Internal functions

-spec wrap_msg(connection(), non_neg_integer(), binary()) -> binary().
wrap_msg(Handle, MsgId, Msg) ->
    IDLen = pipesock_conn:get_len(Handle),
    <<MsgId:IDLen, Msg/binary>>.

-spec unwrap_msg(connection(), binary()) -> binary().
unwrap_msg(Handle, Msg) ->
    IDLen = pipesock_conn:get_len(Handle),
    <<_:IDLen, RealMsg/binary>> = Msg,
    RealMsg.