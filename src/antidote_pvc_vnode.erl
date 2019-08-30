%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(antidote_pvc_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include("pvc.hrl").
-include("debug_log.hrl").

%% Public API
-export([most_recent_vc/1,
         vnodes_ready/0,
         replicas_ready/0]).

%% Protocol API
-export([prepare/5,
         decide/4]).

%% riak_core_vnode callbacks
-export([start_vnode/1,
         init/1,
         handle_command/3,
         handle_coverage/4,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_command/3,
         handle_handoff_data/2,
         encode_handoff_item/2,
         is_empty/1,
         terminate/2,
         handle_exit/3,
         delete/1]).

-type protocol() :: psi | ser.
-type tx_id() :: term().
-type decision() :: abort | {ready, pvc_vc()}.
-type cache(_K, _V) :: ets:tab().

-define(VNODE_MASTER, antidote_pvc_vnode_master).
-define(WRITE_HEAVY_OPTS, [set, public, named_table, {write_concurrency, true}]).
-define(PENDING_OPTS, [bag, public, named_table, {read_concurrency, true}, {write_concurrency, true}]).

%% Called internally by riak_core, or via rpc
-ignore_xref([start_vnode/1,
              replicas_ready/0,
              vnodes_ready/0]).

-record(state, {
    partition :: partition_id(),

    %% number of gen_servers replicating this vnode state
    replicas_n = ?READ_CONCURRENCY :: non_neg_integer(),
    %% Most Recent VC, cached in an ETS
    most_recent_vc :: cache(mrvc, pvc_vc()),

    %% Cache the last committed version of a key
    last_vsn_cache :: cache(key(), non_neg_integer()),
    %% If a key is not present in the above cache, return this
    default_last_vsn = 0 :: non_neg_integer(),

    %% Last prepared counter, increased on each prepare
    last_prepared = 0 :: non_neg_integer(),
    %% Commit queue of pending transactions
    commit_queue = pvc_commit_queue:new() :: pvc_commit_queue:cqueue(),

    %% Holds temporary data for the transactions while they sit
    %% on the commit queue. Used to offload memory to an ETS.
    pending_tx_data :: cache(tx_id(), term()),

    %% A pair of inverted indices over the transactions read/write sets
    %% Used to compute write-write and read-write conflicts in a quick way.
    pending_reads :: cache(key(), tx_id()),
    pending_writes :: cache(key(), tx_id()),

    %% Contains the decisions that clients make about
    %% transactions after a voting phase. Client will write to this table
    %% from outside of the virtual node, to avoid serialization overhead.
    decision_cache :: cache(tx_id(), {protocol(), decision()}),

    dequeue_timer = undefined :: timer:tref() | undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec most_recent_vc(partition_id()) -> pvc_vc().
most_recent_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?MRVC_TABLE), mrvc, 2).

%% @doc For all nodes in the system, report if all the vnodes are ready
-spec vnodes_ready() -> boolean().
vnodes_ready() ->
    Nodes = dc_utilities:get_all_partitions_nodes(),
    ReadyFun = fun(IndexNode) ->
        try
            riak_core_vnode_master:sync_command(IndexNode,
                                                is_ready,
                                                ?VNODE_MASTER,
                                                infinity)
        catch _:_ -> false
        end
    end,
    lists:all(ReadyFun, Nodes).

%% @doc For all nodes in the system, report if the read replicas are online
-spec replicas_ready() -> boolean().
replicas_ready() ->
    Nodes = dc_utilities:get_all_partitions_nodes(),
    ReadyFun = fun(IndexNode) ->
        try
            riak_core_vnode_master:sync_command(IndexNode,
                                                replicas_ready,
                                                ?VNODE_MASTER,
                                                infinity)
        catch _:_ -> false
        end
    end,
    lists:all(ReadyFun, Nodes).

-spec prepare(Partition :: partition_id(),
              Protocol :: protocol(),
              TxId :: tx_id(),
              Payload :: term(),
              Version :: non_neg_integer()) -> {ok, non_neg_integer()} | {error, reason()}.

prepare(Partition, Protocol, TxId, Payload, Version) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {prepare, Protocol, TxId, Payload, Version},
                                        ?VNODE_MASTER,
                                        infinity).

%% FIXME(borja): Implement decide
-spec decide(Partition :: partition_id(),
             Protocol :: protocol(),
             TxId :: tx_id(),
             Outcome :: decision()) -> ok.

decide(_Partition, _Protocol, _TxId, _Outcome) ->
    ok.

%%%===================================================================
%%% riak_core callbacks
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    MRVC = new_cache(Partition, ?MRVC_TABLE),
    true = ets:insert(MRVC, {mrvc, pvc_vclock:new()}),

    State = #state{
        partition       = Partition,
        most_recent_vc  = MRVC,
        last_vsn_cache  = new_cache(Partition, ?LAST_VSN_TABLE),
        pending_tx_data = new_cache(Partition, ?PENDING_DATA_TABLE, ?WRITE_HEAVY_OPTS),
        pending_reads   = new_cache(Partition, ?PENDING_READS_TABLE, ?PENDING_OPTS),
        pending_writes  = new_cache(Partition, ?PENDING_WRITES_TABLE, ?PENDING_OPTS),
        decision_cache  = new_cache(Partition, ?DECIDE_TABLE, ?WRITE_HEAVY_OPTS)
    },

    {ok, State}.

%% Used by antidote to check that vnode is alive
handle_command({hello}, _Sender, State) ->
    {reply, ok, State};

handle_command(is_ready, _Sender, State) ->
    Ready = lists:all(fun is_ready/1, [State#state.last_vsn_cache,
                                       State#state.pending_tx_data,
                                       State#state.pending_reads,
                                       State#state.pending_writes,
                                       State#state.decision_cache]),
    {reply, Ready, State};

handle_command(start_replicas, _From, S = #state{partition=P, replicas_n=N}) ->
    ok = pvc_read_replica:start_replicas(P, N),
    Result = pvc_read_replica:replica_ready(P, N),
    {ok, TRef} = schedule_dequeue_interval(P),
    {reply, Result, S#state{dequeue_timer=TRef}};

handle_command(stop_replicas, _From, S = #state{partition=P, replicas_n=N, dequeue_timer=TRef}) ->
    ok = pvc_read_replica:start_replicas(P, N),
    ok = stop_dequeue_interval(TRef),
    {reply, ok, S#state{dequeue_timer=undefined}};

handle_command(replicas_ready, _From, S = #state{partition=P, replicas_n=N}) ->
    Result = pvc_read_replica:replica_ready(P, N),
    {reply, Result, S};

handle_command(flush_queue, _From, State) ->
    lists:foreach(fun ets:delete_all_objects/1, [State#state.pending_tx_data,
                                                 State#state.pending_reads,
                                                 State#state.pending_writes,
                                                 State#state.decision_cache]),

    {reply, ok, State#state{commit_queue=pvc_commit_queue:new()}};

handle_command({prepare, Protocol, TxId, Payload, Version}, _From, State) ->
    %% FIXME(borja): Implement prepare
    ?LAGER_LOG("{prepare, ~p, ~p, ~p, ~p}", [Protocol, TxId, Payload, Version]),
    {reply, {ok, State#state.last_prepared}, State};

handle_command(dequeue_event, _From, State) ->
    %% FIXME(borja): Implement dequeue
    {noreply, State}.

%%%===================================================================
%%% Util Functions
%%%===================================================================

-spec safe_bin_to_atom(binary()) -> atom().
safe_bin_to_atom(Bin) ->
    case catch binary_to_existing_atom(Bin, latin1) of
        {'EXIT', _} -> binary_to_atom(Bin, latin1);
        Atom -> Atom
    end.

-spec new_cache(partition_id(), atom()) -> cache_id().
new_cache(Partition, Name) ->
    new_cache(Partition, Name, [set, protected, named_table, ?TABLE_CONCURRENCY]).

new_cache(Partition, Name, Options) ->
    CacheName = cache_name(Partition, Name),
    case ets:info(CacheName) of
        undefined ->
            ets:new(CacheName, Options);
        _ ->
            lager:info("Unsable to create cache ~p at ~p, retrying", [Name, Partition]),
            timer:sleep(100),
            try ets:delete(CacheName) catch _:_ -> ok end,
            new_cache(Partition, Name, Options)
    end.

-spec cache_name(partition_id(), atom()) -> cache_id().
cache_name(Partition, Name) ->
    BinNode = atom_to_binary(node(), latin1),
    BiName = atom_to_binary(Name, latin1),
    BinPart = integer_to_binary(Partition),
    TableName = <<BiName/binary, <<"-">>/binary, BinPart/binary, <<"@">>/binary, BinNode/binary>>,
    safe_bin_to_atom(TableName).

-spec is_ready(cache_id()) -> boolean().
is_ready(Table) ->
    undefined =/= ets:info(Table).

%% @doc Schedule a dequeue event in the vnode
%%
%%      This function will send a message to the Partition's vnode every
%%      DEQUEUE_INTERVAL ms
-spec schedule_dequeue_interval(partition_id()) -> {ok, timer:tref()}.
schedule_dequeue_interval(Partition) ->
    Args = [{Partition, node()}, dequeue_event, ?VNODE_MASTER],
    timer:apply_interval(?DEQUEUE_INTERVAL, riak_core_vnode_master,
                                            command,
                                            Args).

-spec stop_dequeue_interval(timer:tref()) -> ok.
stop_dequeue_interval(TRef) ->
    {ok, cancel} = timer:cancel(TRef),
    ok.

%%%===================================================================
%%% stub riak_core callbacks
%%%===================================================================

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handoff_starting(_, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_, State) ->
    {ok, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_data(_Arg0, _Arg1) ->
    erlang:error(not_implemented).

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

delete(State) ->
    {ok, State}.
