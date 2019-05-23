-module(pvc_transaction_writeset).

-include("pvc.hrl").

%% The transaction writeset is divided in three layers
%% for easier batching during the 2pc phase
-type inner_writeset() :: pvc_writeset:ws(term(), term()).
-type partition_writesets() :: orddict:orddict(partition_id(), inner_writeset()).
-type ws() :: orddict:orddict(node_ip(), partition_writesets()).

-export_type([ws/0, partition_writesets/0, inner_writeset/0]).

%% API
-export([new/0,
         put/4,
         fold/3]).

-spec new() -> ws().
new() ->
    orddict:new().

-spec put(index_node(), term(), term(), ws()) -> ws().
put({Partition, Node}, Key, Val, Dict) ->
    %% Peel downwards
    PartitionWritesets0 = find_outer(Node, Dict),
    Writeset0 = find_inner(Partition, PartitionWritesets0),
    %% Update updwards
    Writeset1 = pvc_writeset:put(Key, Val, Writeset0),
    PartitionWritesets1 = orddict:store(Partition, Writeset1, PartitionWritesets0),
    orddict:store(Node, PartitionWritesets1, Dict).

-spec find_outer(term(), ws()) -> partition_writesets().
find_outer(Key, Dict) ->
    find_or_else(Key, orddict:new(), Dict).

-spec find_inner(term(), partition_writesets()) -> inner_writeset().
find_inner(Key, Dict) ->
    find_or_else(Key, pvc_writeset:new(), Dict).

-spec find_or_else(Key :: term(), Defaut :: term(), Dict :: orddict:orddict()) -> term().
find_or_else(Key, Default, Dict) ->
    case orddict:find(Key, Dict) of
        error -> Default;
        {ok, Val} -> Val
    end.

-spec fold(fun((node_ip(), partition_writesets(), term()) -> term()), term(), ws()) -> term().
fold(Fun, Acc, WS) ->
    orddict:fold(Fun, Acc, WS).
