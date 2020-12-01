-type node_ip() :: atom() | inet:ip_address().
-type socket_error() :: {error, inet:posix()}.
-type unique_nodes() :: ordsets:ordset(node_ip()).
-type partition_id() :: non_neg_integer().
-type index_node() :: {partition_id(), node_ip()}.
-type replica_id() :: term().

-define(ANTIDOTE_BUCKET, <<"antidote">>).
-define(GRB_BUCKET, <<"grb">>).

%% TCP options for bootstrap info
-define(UTIL_CONN_OPTS, [binary,
                         {active, false},
                         {packet, 4},
                         {nodelay, true}]).

-define(RED_CONN_OPTS, [binary,
                       {active, once},
                       {packet, 4},
                       {nodelay, true}]).

-export_type([node_ip/0,
              socket_error/0,
              unique_nodes/0,
              partition_id/0,
              index_node/0,
              replica_id/0]).
