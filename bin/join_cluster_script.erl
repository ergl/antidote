#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name join_cluster@127.0.0.1 -setcookie antidote

-mode(compile).

-export([main/1]).

main(NodesListString) ->
    case parse_node_list(NodesListString) of
        error ->
            usage();

        {ok, [_SingleNode]} ->
            io:format("Single-node cluster, nothing to join"),
            halt();

        {ok, Nodes} ->
            io:format("Starting clustering of nodes ~p~n", [Nodes]),
            lists:foreach(fun(N) -> erlang:set_cookie(N, antidote) end, Nodes),

            ok = join_cluster(Nodes),

            rpc:multicall(Nodes, inter_dc_manager, start_bg_processes, [stable], infinity),
            io:format("Successfully joined nodes ~p~n", [Nodes])
    end.

-spec parse_node_list(list(string())) -> {ok, [node()]} | error.
parse_node_list([]) ->
    error;

parse_node_list([_|_]=NodeListString) ->
    try
        Nodes = lists:foldl(fun(NodeString, Acc) ->
            Node = list_to_atom(NodeString),
            [Node | Acc]
        end, [], NodeListString),
        {ok, lists:reverse(Nodes)}
    catch
        _:_ -> error
    end.

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(standard_error, "~s 'node_1@host_1' ... 'node_n@host_n'~n", [Name]),
    halt(1).

%% @doc Build clusters out of the given node list
-spec join_cluster(list(atom())) -> ok.
join_cluster([MainNode | OtherNodes] = Nodes) ->
    ok = case check_nodes_own_their_ring(Nodes) of
         ok ->
             ok;
        {error, FaultyNode, Reason} ->
            io:fwrite(standard_error, "Bad node ~s on ownership check with reason ~p", [FaultyNode, Reason]),
            halt(1)
    end,

    %% Do a plan/commit staged join, instead of sequential joins
    ok = lists:foreach(fun(N) -> request_join(N, MainNode) end, OtherNodes),
    ok = wait_plan_ready(MainNode),
    ok = commit_plan(MainNode),
    ok = try_cluster_ready(Nodes),

    ok = wait_until_nodes_ready(Nodes),

    %% Ensure each node owns a portion of the ring
    ok = wait_until_nodes_agree_about_ownership(Nodes),
    ok = wait_until_no_pending_changes(Nodes),
    ok = wait_until_ring_converged(Nodes),
    ok = wait_until_master_ready(MainNode).

%% @doc Ensure that all nodes are the sole owner of their rings
-spec check_nodes_own_their_ring(list(atom())) -> ok | {error, atom()}.
check_nodes_own_their_ring([]) -> ok;
check_nodes_own_their_ring([H | T]) ->
    case sorted_ring_owners(H) of
        {ok, [H]} ->
            check_nodes_own_their_ring(T);
        Reason ->
            {error, H, Reason}
    end.

%% @doc Retrieve a list of ring-owning physical nodes according to the MainNode
%%
%%      A node is ring-owning if a partition is stored on it
%%
-spec sorted_ring_owners(node()) -> {ok, list(node())} | {badrpc, term()}.
sorted_ring_owners(Node) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Owners = [Owner || {_Idx, Owner} <- rpc:call(Node, riak_core_ring, all_owners, [Ring])],
            SortedOwners = lists:usort(Owners),
            io:format("Owners at ~p: ~p~n", [Node, SortedOwners]),
            {ok, SortedOwners};

        {badrpc, _}=BadRpc ->
            BadRpc
    end.

-spec owners_according_to(node(), node()) -> {ok, list(node())} | {badrpc, term()}.
owners_according_to(Node, MainNode) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            Owners = [Owner || {_Idx, Owner} <- rpc:call(MainNode, riak_core_ring, all_owners, [Ring])],
            SortedOwners = lists:usort(Owners),
            io:format("Owners at ~p: ~p (according to ~p)~n", [Node, SortedOwners, MainNode]),
            {ok, SortedOwners};

        {badrpc, _}=BadRpc ->
            BadRpc
    end.

%% @doc Make `Node` request joining with `MasterNode`
-spec request_join(node(), node()) -> ok.
request_join(Node, MasterNode) ->
    timer:sleep(5000),
    R = rpc:call(Node, riak_core, staged_join, [MasterNode]),
    io:format("[join request] ~p to ~p: (result ~p)~n", [Node, MasterNode, R]),
    ok.

-spec wait_plan_ready(node()) -> ok.
wait_plan_ready(Node) ->
    io:format("[ring plan] Will start plan on ~p~n", [Node]),
    case rpc:call(Node, riak_core_claimant, plan, []) of
        {error, ring_not_ready} ->
            io:format("[ring plan] Ring not ready, retrying...~n"),
            timer:sleep(5000),
            ok = wait_until_no_pending_changes(Node),
            wait_plan_ready(Node);

        {ok, _, _} ->
            ok
    end.

-spec commit_plan(node()) -> ok.
commit_plan(Node) ->
    io:format("[ring commit] Will start commit on ~p~n", [Node]),
    case rpc:call(Node, riak_core_claimant, commit, []) of
        {error, plan_changed} ->
            io:format("[ring commit] Plan changed, retrying...~n"),
            timer:sleep(100),
            ok = wait_until_no_pending_changes(Node),
            ok = wait_plan_ready(Node),
            commit_plan(Node);

        {error, ring_not_ready} ->
            io:format("[ring commit] Ring not ready, retrying...~n"),
            timer:sleep(100),
            wait_until_no_pending_changes(Node),
            commit_plan(Node);

        {error, nothing_planned} ->
            %% Assume plan actually committed somehow
            ok;

        ok ->
            ok
    end.

%% @doc Given a list of nodes, wait until all nodes believe there are no
%%      on-going or pending ownership transfers.
%%
-spec wait_until_no_pending_changes([node()]) -> ok | fail.
wait_until_no_pending_changes([MainNode | _] = Nodes) when is_list(Nodes) ->
    NoPendingHandoffs = fun() ->
        io:format("Check no pending handoffs..."),
        rpc:multicall(Nodes, riak_core_vnode_manager, force_handoffs, []),
        {Rings, BadNodes} = rpc:multicall(Nodes, riak_core_ring_manager, get_raw_ring, []),
        io:format("Check no pending handoffs (badnodes: ~p)...~n", [BadNodes]),
        case BadNodes of
            [] ->
                lists:all(fun({ok, Ring}) ->
                    [] =:= rpc:call(MainNode, riak_core_ring, pending_changes, [Ring])
                end, Rings);

            _ ->
                false
        end
    end,

    wait_until(NoPendingHandoffs);

wait_until_no_pending_changes(Node) ->
    wait_until_no_pending_changes([Node]).

%% @doc Utility function used to construct test predicates. Retries the
%%      function `Fun' until it returns `true', or until the maximum
%%      number of retries is reached.
%%
%% @TODO Use config for this
-spec wait_until(fun(() -> boolean())) -> ok | {fail, boolean()}.
wait_until(Fun) when is_function(Fun) ->
    MaxTime = 600000,
    Delay = 1000,
    Retry = MaxTime div Delay,
    wait_until(Fun, Retry, Delay).

-spec wait_until(
    fun(() -> boolean()),
    non_neg_integer(),
    non_neg_integer()
) -> ok | {fail, boolean()}.

wait_until(Fun, Retry, Delay) when Retry > 0 ->
    wait_until_result(Fun, true, Retry, Delay).

-spec wait_until_result(
    fun(() -> any()),
    any(),
    non_neg_integer(),
    non_neg_integer()
) -> ok | {fail, any()}.

wait_until_result(Fun, Result, Retry, Delay) when Retry > 0 ->
    Res = Fun(),
    case Res of
        Result ->
            ok;

        _ when Retry == 1 ->
            {fail, Res};

        _ ->
            timer:sleep(Delay),
            wait_until_result(Fun, Result, Retry-1, Delay)
    end.

%% @doc Wait until all nodes agree about ready nodes in their rings
-spec try_cluster_ready([node()]) -> ok.
try_cluster_ready(Nodes) ->
    try_cluster_ready(Nodes, 3, 500).

-spec try_cluster_ready([node()], non_neg_integer(), non_neg_integer()) -> ok.
try_cluster_ready([MainNode | _] = _Nodes, 0, _SleepMs) ->
    io:format("[cluster ready] Still not ready, will retry plan~n"),
    ok = wait_plan_ready(MainNode),
    commit_plan(MainNode);

try_cluster_ready([MainNode | _] = Nodes, Retries, SleepMs) ->
    AllReady = lists:all(fun(Node) -> is_ready(Node, MainNode) end, Nodes),
    case AllReady of
        true ->
            ok;
        false ->
            timer:sleep(SleepMs),
            try_cluster_ready(Nodes, Retries - 1, SleepMs)
    end.

%% @doc Check if `Node` is ready according to `MainNode`
%% @private
-spec is_ready(node(), node()) -> boolean().
is_ready(Node, MainNode) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            ReadyMembers = rpc:call(MainNode, riak_core_ring, ready_members, [Ring]),
            lists:member(Node, ReadyMembers);

        _ ->
            false
    end.

%% @doc Given a list of nodes, wait until all nodes are considered ready.
%%
%%      See {@link wait_until_ready/1} for definition of ready.
%%
-spec wait_until_nodes_ready([node()]) -> ok.
wait_until_nodes_ready([MainNode | _] = Nodes) ->
    true = lists:all(fun(Node) ->
        case wait_until(fun() -> is_ready(Node, MainNode) end) of
            ok ->
                true;
            Res ->
                io:format("wait_until_nodes_ready got ~p~n", [Res]),
                false
        end
    end, Nodes),
    ok.

%% @doc Wait until all nodes agree about all ownership views
-spec wait_until_nodes_agree_about_ownership([node()]) -> boolean().
wait_until_nodes_agree_about_ownership(Nodes) ->
    SortedNodes = lists:usort(Nodes),
    true = lists:all(fun(Node) ->
        Res = wait_until(fun() ->
            case sorted_ring_owners(Node) of
                {ok, SortedNodes} ->
                    true;
                _ ->
                    false
            end
        end),
        case Res of
            ok ->
                true;
            Res ->
                io:format("wait_until_nodes_agree_about_ownership got ~p~n", [Res]),
                false
        end
    end, Nodes),
    ok.

%% @doc Given a list of nodes, wait until all nodes believe the ring has
%%      converged (ie. `riak_core_ring:is_ready' returns `true').
-spec wait_until_ring_converged([node()]) -> ok.
wait_until_ring_converged([MainNode | _] = Nodes) ->
    true = lists:all(fun(Node) ->
        case wait_until(fun() -> is_ring_ready(Node, MainNode) end) of
            ok ->
                true;
            Res ->
                io:format("wait_until_ring_converged got ~p~n", [Res]),
                false
        end
    end, Nodes),
    ok.

%% @private
is_ring_ready(Node, MainNode) ->
    case rpc:call(Node, riak_core_ring_manager, get_raw_ring, []) of
        {ok, Ring} ->
            rpc:call(MainNode, riak_core_ring, ring_ready, [Ring]);

        _ ->
            false
    end.

-spec wait_until_master_ready(node()) -> ok.
wait_until_master_ready(MasterNode) ->
    wait_until(fun() -> check_ready(MasterNode) end).

%% @doc This function provides the same functionality as wait_ready_nodes
%% except it takes as input a sinlge physical node instead of a list
-spec check_ready(node()) -> boolean().
check_ready(Node) ->
    io:format("[master ready] Checking ~p~n", [Node]),

    VNodeReady = rpc:call(Node, clocksi_vnode, check_tables_ready, []),
    ReadServersReady = rpc:call(Node, clocksi_vnode, check_servers_ready, []),
    PVCReplicaReady = rpc:call(Node, pvc_read_replica, all_ready, []),
    MaterializerReady = rpc:call(Node, materializer_vnode, check_tables_ready, []),
    StableMetadataReady = rpc:call(Node, stable_meta_data_server, check_tables_ready, []),

    NodeReady = VNodeReady
        andalso ReadServersReady
        andalso PVCReplicaReady
        andalso MaterializerReady
        andalso StableMetadataReady,

    case NodeReady of
        true ->
            io:format("Node ~w is ready! ~n~n", [Node]);
        false ->
            io:format("Node ~w is not ready ~n~n", [Node])
    end,

    NodeReady.
