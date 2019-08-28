-module(pvc_ser_transaction_readwriteset).

-include("pvc.hrl").

-type rs() :: #{term() => non_neg_integer()}.
-type ws() :: #{term() => term()}.

-type inner_set() :: {rs(), ws()}.
-type partitions_readwriteset() :: #{partition_id() => inner_set()}.
-type rws() :: #{node_ip() => partitions_readwriteset()}.
-export_type([rws/0,partitions_readwriteset/0,inner_set/0]).

%% API
-export([new/0,
         put_ws/4,
         put_rs/4,
         fold/3]).

-spec new() -> rws().
new() ->
    #{}.

-spec put_ws(index_node(), term(), term(), rws()) -> rws().
put_ws(IndexNode, Key, Val, Map) ->
    put_generic(IndexNode, Key, Val, write_set, Map).

-spec put_rs(index_node(), term(), non_neg_integer(), rws()) -> rws().
put_rs(IndexNode, Key, Version, Map) ->
    put_generic(IndexNode, Key, Version, read_set, Map).

put_generic({Partition, Node}, Key, Val, Set, Map) ->
    %% Peel downwards
    PRWS0 = maps:get(Node, Map, #{}),
    InnerSet0 = maps:get(Partition, PRWS0, {#{}, #{}}),

    %% Update updwards
    InnerSet1 = update_with(InnerSet0, Set, Key, Val),
    PRWS1 = maps:put(Partition, InnerSet1, PRWS0),
    maps:put(Node, PRWS1, Map).

update_with({Target, Right}, read_set, Key, Val) ->
    {maps:put(Key, Val, Target), Right};

update_with({Left, Target}, write_set, Key, Val) ->
    {Left, maps:put(Key, Val, Target)}.

-spec fold(Fun :: fun((node_ip(), partitions_readwriteset(), term()) -> term()),
           Acc :: term(),
           RWS :: rws()) -> term().

fold(Fun, Acc, RWS) ->
    maps:fold(Fun, Acc, RWS).
