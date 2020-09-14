-module(pvc_grb_rws).

-include("pvc.hrl").

%% todo(borja): add operations
-type rs() :: #{term() => non_neg_integer()}.
%% todo(borja): Convert ws to a sequence when adding ops
-type ws() :: #{term() => term()}.

-type inner_set() :: {rs(), ws()}.
-type partitions_readwriteset() :: #{partition_id() => inner_set()}.
-type t() :: #{node_ip() => partitions_readwriteset()}.
-export_type([t/0, partitions_readwriteset/0, inner_set/0]).

%% API
-export([new/0,
         put_op/5,
         put_ronly_op/4,
         fold/3,
         make_red_prepares/1]).

-spec new() -> t().
new() ->
    #{}.

-spec put_ronly_op(index_node(), term(), non_neg_integer(), t()) -> t().
put_ronly_op({Partition, Node}, Key, RedTS, Map) ->
    %% Peel downwards
    PRWS0 = maps:get(Node, Map, #{}),
    {InnerRS, WS} = maps:get(Partition, PRWS0, {#{}, #{}}),
    %% Update updwards
    InnerSet1 = {maps:put(Key, RedTS, InnerRS), WS},
    PRWS1 = maps:put(Partition, InnerSet1, PRWS0),
    maps:put(Node, PRWS1, Map).

-spec put_op(index_node(), term(), term(), non_neg_integer(), t()) -> t().
put_op({Partition, Node}, Key, Val, RedTS, Map) ->
    %% Peel downwards
    PRWS0 = maps:get(Node, Map, #{}),
    {InnerRS, InnerWS} = maps:get(Partition, PRWS0, {#{}, #{}}),
    %% Update updwards
    InnerSet1 = {maps:put(Key, RedTS, InnerRS), maps:put(Key, Val, InnerWS)},
    PRWS1 = maps:put(Partition, InnerSet1, PRWS0),
    maps:put(Node, PRWS1, Map).

-spec fold(Fun :: fun((node_ip(), partitions_readwriteset(), term()) -> term()),
    Acc :: term(),
    RWS :: t()) -> term().

fold(Fun, Acc, RWS) ->
    maps:fold(Fun, Acc, RWS).

-spec make_red_prepares(t()) -> [{partition_id(), rs(), ws()}].
make_red_prepares(RWS) ->
    fold(fun(_, Inner, Acc0) ->
        fold(fun(P, {RS, WS}, Acc) ->
            [{P, RS, WS} | Acc]
        end, Acc0, Inner)
    end, [], RWS).
