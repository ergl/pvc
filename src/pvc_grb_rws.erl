-module(pvc_grb_rws).

-include("pvc.hrl").

-type rs() :: #{term() => undefined}.
-type ws() :: #{term() => grb_lww:op() | grb_gset:op()}.

-type inner_set() :: {rs(), ws()}.
-type partitions_readwriteset() :: #{partition_id() => inner_set()}.
-type t() :: #{node_ip() => partitions_readwriteset()}.
-export_type([t/0, partitions_readwriteset/0, inner_set/0]).

%% API
-export([new/0,
         add_operation/5,
         add_read_key/3,
         fold_updated_partitions/3,
         make_red_prepares/1]).

-spec new() -> t().
new() ->
    #{}.

-spec add_read_key(index_node(), term(), t()) -> t().
add_read_key({Partition, Node}, Key, Map) ->
    Inner = maps:get(Node, Map, #{}),
    Map#{Node => Inner#{Partition =>
        case maps:get(Partition, Inner, undefined) of
            undefined ->
                { #{Key => undefined}, #{} };
            {RS, WS} ->
                { RS#{Key => undefined}, WS }
        end
    }}.

-spec add_operation(index_node(), term(), module(), term(), t()) -> t().
add_operation({Partition, Node}, Key, Mod, Operation, Map) ->
    Inner = maps:get(Node, Map, #{}),
    Map#{Node => Inner#{Partition =>
        case maps:get(Partition, Inner, undefined) of
            undefined ->
                { #{Key => undefined}, #{Key => Operation} };

            {RS, WS = #{Key := PrevOp}} ->
                {
                    RS#{Key => undefined},
                    WS#{Key := Mod:merge_ops(PrevOp, Operation)}
                };

            {RS, WS} ->
                {
                    RS#{Key => undefined},
                    WS#{Key => Operation}
                }
        end
    }}.

-spec fold_updated_partitions(Fun :: fun((node_ip(), [partition_id()], term()) -> term()),
                              Acc :: term(),
                              RWS :: t()) -> term().

fold_updated_partitions(Fun, Acc, RWS) when is_function(Fun, 3) ->
    fold_updated_partitions_(Fun, maps:iterator(RWS), Acc).

fold_updated_partitions_(Fun, Iterator, ClientAcc) ->
    case maps:next(Iterator) of
        none ->
            ClientAcc;
        {Node, Inner, Next} ->
            Partitions = maps:fold(fun
                (Partition, {_, WS}, Acc) when map_size(WS) =/= 0 ->
                    [Partition | Acc];
                (_, _, Acc) -> Acc
            end, [], Inner),
            fold_updated_partitions_(Fun, Next, Fun(Node, Partitions, ClientAcc))
    end.

-spec make_red_prepares(t()) -> [{partition_id(), [term()], ws()}].
make_red_prepares(RWS) ->
    maps:fold(fun(_Node, Inner, Acc0) ->
        maps:fold(fun(Partition, {RS, WS}, Acc) ->
            [{Partition, maps:keys(RS), WS} | Acc]
        end, Acc0, Inner)
    end, [], RWS).
