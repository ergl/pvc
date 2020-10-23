-module(pvc_grb_rws).

-include("pvc.hrl").

%% todo(borja): add operations
-type rs() :: #{term() => undefined}.
%% todo(borja): Convert ws to a sequence when adding ops
-type ws() :: #{term() => term()}.

-type inner_set() :: {rs(), ws()}.
-type partitions_readwriteset() :: #{partition_id() => inner_set()}.
-type t() :: #{node_ip() => partitions_readwriteset()}.
-export_type([t/0, partitions_readwriteset/0, inner_set/0]).

%% API
-export([new/0,
         put_op/4,
         put_ronly_op/3,
         fold_updates/3,
         make_red_prepares/1]).

-spec new() -> t().
new() ->
    #{}.

-spec put_ronly_op(index_node(), term(), t()) -> t().
put_ronly_op({Partition, Node}, Key, Map) ->
    Inner = maps:get(Node, Map, #{}),
    Map#{Node => Inner#{Partition =>
        case maps:get(Partition, Inner, undefined) of
            undefined ->
                { #{Key => undefined}, #{} };
            {RS, WS} ->
                { RS#{Key => undefined}, WS }
        end
    }}.

-spec put_op(index_node(), term(), term(), t()) -> t().
put_op({Partition, Node}, Key, Val, Map) ->
    Inner = maps:get(Node, Map, #{}),
    Map#{Node => Inner#{Partition =>
        case maps:get(Partition, Inner, undefined) of
            undefined ->
                { #{Key => undefined}, #{Key => Val} };
            {RS, WS} ->
                { RS#{Key => undefined}, WS#{Key => Val} }
        end
    }}.

-spec fold_updates(Fun :: fun((node_ip(), [partition_id()], [{partition_id(), ws()}], term()) -> term()),
                   Acc :: term(),
                   RWS :: t()) -> term().

fold_updates(Fun, Acc, RWS) when is_function(Fun, 4) ->
    fold_updates_(Fun, maps:iterator(RWS), Acc).

fold_updates_(Fun, Iterator, ClientAcc) ->
    case maps:next(Iterator) of
        none ->
            ClientAcc;
        {Node, Inner, Next} ->
            {Updated, Partitions} = maps:fold(fun
                (Partition, {_, WS}, {PrepareAcc, PartitionAcc}) when map_size(WS) =/= 0 ->
                    {
                        [{Partition, WS} | PrepareAcc],
                        [Partition | PartitionAcc]
                    };
                (_, _, Acc) -> Acc
            end, {[], []}, Inner),
            fold_updates_(Fun, Next, Fun(Node, Partitions, Updated, ClientAcc))
    end.

-spec make_red_prepares(t()) -> [{partition_id(), [term()], ws()}].
make_red_prepares(RWS) ->
    maps:fold(fun(_Node, Inner, Acc0) ->
        maps:fold(fun(Partition, {RS, WS}, Acc) ->
            [{Partition, maps:keys(RS), WS} | Acc]
        end, Acc0, Inner)
    end, [], RWS).
