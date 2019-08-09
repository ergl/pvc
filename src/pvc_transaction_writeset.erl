-module(pvc_transaction_writeset).

-include("pvc.hrl").

%% The transaction writeset is divided in three layers
%% for easier batching during the 2pc phase
-type partitions_writeset() :: #{partition_id() => #{term() => term()}}.
-type ws() :: #{node_ip() => partitions_writeset()}.
-export_type([ws/0,partitions_writeset/0]).

%% API
-export([new/0,
         put/4,
         fold/3]).

-spec new() -> ws().
new() ->
    #{}.

-spec put(index_node(), term(), term(), ws()) -> ws().
put({Partition, Node}, Key, Val, Dict) ->
    %% Peel downwards
    PartitionWritesets0 = maps:get(Node, Dict, #{}),
    Writeset0 = maps:get(Partition, PartitionWritesets0, #{}),
    %% Update updwards
    Writeset1 = maps:put(Key, Val, Writeset0),
    PartitionWritesets1 = maps:put(Partition, Writeset1, PartitionWritesets0),
    maps:put(Node, PartitionWritesets1, Dict).

-spec fold(Fun :: fun((node_ip(), partitions_writeset(), term()) -> term()),
           Acc :: term(),
           WS :: ws()) -> term().

fold(Fun, Acc, WS) ->
    maps:fold(Fun, Acc, WS).
