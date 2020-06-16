-type node_ip() :: atom() | inet:ip_address().
-type socket_error() :: {error, inet:posix()}.
-type unique_nodes() :: ordsets:ordset(node_ip()).
-type partition_id() :: non_neg_integer().
-type index_node() :: {partition_id(), node_ip()}.
-type replica_id() :: term().

-define(ANTIDOTE_BUCKET, <<"antidote">>).
-define(GRB_BUCKET, <<"grb">>).
