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

-module(pvc_storage_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").

%% vnode management API
-export([get_cache_name/2,
         check_tables_ready/0]).

%% API
-export([prepare/3,
         decide/3,
         get_max_vc/3,
         get_most_recent_vc/2,
         read_key/4,
         read_range/4]).

%% riak core callbacks
-export([start_vnode/1,
         init/1,
         handle_command/3,
         handle_coverage/4,
         handle_exit/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_command/3,
         handle_handoff_data/2,
         encode_handoff_item/2,
         is_empty/1,
         terminate/2,
         delete/1]).

-ignore_xref([start_vnode/1]).

-record(state, {
    partition :: partition_id(),

    %% The number of read replicas of this partition storage
    n_replicas :: non_neg_integer(),

    %% ETS table containing the state of this partition
    %% Contains the current MostRecentVC
    %% and the SeqNumber
    partition_state:: cache_id(),

    %% ETS table mapping keys -> last version
    %% Used to compute VLog.last(key) without
    %% traversing the VLog
    last_committed_version_cache :: cache_id(),

    %% ETS table mapping keys -> VLog
    storage :: cache_id(),

    %% Ordered ETS table to support range queries
    %% We don't actually store values here, just keys,
    %% so we just use the ordered ETS as a shared-memory ordered set
    %%
    %% The keys present here are used to build a "range" of keys,
    %% that will be read later using the normal read procedure
    range_storage :: cache_id(),

    %% Commit Queue at this partition
    commit_queue :: pvc_commit_queue:cqueue(),

    %% Store commits in memory for the CLog
    %% We should find a way to cleanup, otherwise
    %% this will keep growing with each committed tx
    %% FIXME(borja): Hot memory point
    clog :: pvc_commit_log:clog()
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition]) ->
    State = init_state(Partition),
    CommittedCache = open_table(Partition, pvc_committed_cache),
    Storage = open_table(Partition, pvc_storage),
    RangeStorage = open_table(Partition,
                              pvc_range_storage,
                              [ordered_set, protected, named_table, ?TABLE_CONCURRENCY]),

    CommitQueue = pvc_commit_queue:new(),

    CLog = pvc_commit_log:new_at(Partition),

    {ok, #state{partition=Partition,
                n_replicas=?READ_CONCURRENCY,
                partition_state=State,
                last_committed_version_cache=CommittedCache,
                storage=Storage,
                range_storage=RangeStorage,
                commit_queue=CommitQueue,
                clog=CLog}}.

init_state(Partition) ->
    State = open_table(Partition, pvc_partition_state),
    ets:insert(State, [{seq_number, 0}, {mrvc, pvc_vclock:new()}]),
    State.

-spec open_table(partition_id(), atom()) -> cache_id().
open_table(Partition, Name) ->
    open_table(Partition, Name, [set, protected, named_table, ?TABLE_CONCURRENCY]).

-spec open_table(partition_id(), atom(), Options :: [term()]) -> cache_id().
open_table(Partition, Name, Options) ->
    CacheName = get_cache_name(Partition, Name),
    case ets:info(CacheName) of
        undefined ->
            ets:new(CacheName, Options);
        _ ->
            %% Other vnode hasn't finished closing tables
            lager:debug("Unable to open ets table in pvc_storage_vnode, retrying"),
            timer:sleep(100),
            try
                ets:delete(CacheName)
            catch _:_Reason ->
                ok
            end,
            open_table(Partition, Name, Options)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% vnode management API

%% @doc Get an unique ETS name at this partition
-spec get_cache_name(partition_id(), atom()) -> atom().
get_cache_name(Partiton, TableName) ->
    BinTableName = atom_to_binary(TableName, latin1),
    BinPart = binary:encode_unsigned(Partiton),
    Name = <<BinTableName/binary, "-", BinPart/binary>>,
    try
        binary_to_existing_atom(Name, latin1)
    catch _:_ ->
        binary_to_atom(Name, latin1)
    end.

check_tables_ready() ->
    Nodes = dc_utilities:get_all_partitions_nodes(),
    check_tables_ready(Nodes).

check_tables_ready([]) ->
    true;

check_tables_ready([IndexNode | Rest]) ->
    Res = try
        riak_core_vnode_master:sync_command(IndexNode,
                                            tables_ready,
                                            pvc_storage_vnode_master,
                                            ?OP_TIMEOUT)
    catch _:_Reason ->
        %% If virtual node is not up and runnning for any reason, return false
        false
    end,
    case Res of
        false ->
            false;
        true ->
            check_tables_ready(Rest)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% API

%% @doc Send a prepare message to all the given partitions
-spec prepare(pvc_fsm:partitions(), txid(), pvc_vc()) -> ok.
prepare(Partitions, TxId, CommitVC) ->
    lists:foreach(fun({Node, WS}) ->
        riak_core_vnode_master:command(
            Node,
            {prepare, TxId, WS, CommitVC},
            {fsm, undefined, self()},
            pvc_storage_vnode_master)
    end, Partitions).

%% @doc Send a decide message to all the given partitions
-spec decide([{index_node(), [key()]}], txid(), term()) -> ok.
decide(Partitions, TxId, Outcome) ->
    lists:foreach(fun({Node, IndexKeySet}) ->
        riak_core_vnode_master:command(
            Node,
            {decide, TxId, IndexKeySet, Outcome},
            {fsm, undefined, self()},
            pvc_storage_vnode_master)
    end, Partitions).

-spec schedule_queue(partition_id()) -> ok.
schedule_queue(Partition) ->
    riak_core_vnode_master:command(
        {Partition, node()},
        process_queue,
        pvc_storage_vnode_master
    ).

-spec get_max_vc(index_node(), [partition_id()], pvc_vc()) -> pvc_vc().
get_max_vc(IndexNode, ReadPartitions, VCaggr) ->
    riak_core_vnode_master:sync_command(
        IndexNode,
        {max_vc_scan, ReadPartitions, VCaggr},
        pvc_storage_vnode_master,
        infinity
    ).

-spec get_most_recent_vc(index_node(), atom()) -> pvc_vc().
get_most_recent_vc(Node, TableName) ->
    try
        get_mrvc(TableName)
    catch _:_ ->
        riak_core_vnode_master:sync_command(Node, mostrecentvc, pvc_storage_vnode_master)
    end.

-spec read_key(partition_id(), key(), pvc_vc(), atom() | cache_id()) -> {val(), pvc_vc()}.
read_key(Partition, Key, SnapshotTime, Storage) ->
    case ets:info(Storage) of
        undefined ->
            riak_core_vnode_master:sync_command(
                {Partition, node()},
                {read_key, Key, SnapshotTime},
                pvc_storage_vnode_master,
                infinity
            );

        _ ->
            internal_read_key(Key, SnapshotTime, Storage)
    end.

-spec read_range(index_node(), key(), pvc_indices:range(), non_neg_integer()) -> [key()].
read_range({Partiton, _}=Node, Root, Range, Limit) ->
    RangeStorage = get_cache_name(Partiton, pvc_range_storage),
    case ets:info(RangeStorage) of
        undefined ->
            riak_core_vnode_master:sync_command(
                Node,
                {read_range, Root, Range, Limit},
                pvc_storage_vnode_master,
                infinity
            );
        _ ->
            internal_read_range(Root, Range, Limit, RangeStorage)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Handle Riak core commands

handle_command({hello}, _Sender, State) ->
    {reply, ok, State};

%% Check that all the ETS tables in the vnode are up and running
handle_command(tables_ready, _Sender, State=#state{partition=Partition}) ->
    StateReady = ready(Partition, pvc_partition_state),
    CommitReady = ready(Partition, pvc_committed_cache),
    StorageReady = ready(Partition, pvc_storage),
    RangeReady = ready(Partition, pvc_range_storage),

    Ready = StateReady
            andalso CommitReady
            andalso StorageReady
            andalso RangeReady,

    {reply, Ready, State};

handle_command(mostrecentvc, _Sender, State) ->
    {reply, get_mrvc(State#state.partition_state), State};

handle_command({max_vc_scan, ReadPartitions, VCaggr}, _Sender, State) ->
    {reply, pvc_commit_log:get_smaller_from_dots(ReadPartitions, VCaggr, State#state.clog), State};

%% Start all read-replicas of this vnode
handle_command(read_replica_ready, _Sender, State=#state{partition=Partition,
                                                         n_replicas=N}) ->

    ok = pvc_read_replica:start_replicas(Partition, N),
    Node = node(),
    Result = pvc_read_replica:replica_ready(Node, Partition, N),
    {reply, Result, State};

handle_command({read_key, Key, SnapshotTime}, _Sender, State) ->
    {reply, internal_read_key(Key, SnapshotTime, State#state.storage), State};

handle_command({read_range, Root, Range, Limit}, _Sender, State) ->
    {reply, internal_read_range(Root, Range, Limit, State#state.range_storage), State};

handle_command({prepare, TxId, WriteSet, CommitVC}, _Sender, State) ->
    {Vote, NewState} = prepare_internal(TxId, WriteSet, CommitVC, State),
    {reply, Vote, NewState};

handle_command({decide, TxId, IndexKeySet, Outcome}, _Sender, State) ->
    NewState = decide_internal(TxId, IndexKeySet, Outcome, State),
    {noreply, NewState};

handle_command(process_queue, _Sender, State) ->
    NewState = process_queue_internal(State),
    {noreply, NewState};

handle_command(Message, _Sender, State) ->
    lager:info("unhandled command ~p", [Message]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Default Riak Core callbacks

start_vnode(I) -> riak_core_vnode_master:get_vnode_pid(I, ?MODULE).
handle_coverage(_Req, _KeySpaces, _Sender, State) -> {stop, not_implemented, State}.
handoff_starting(_, State) -> {true, State}.
handoff_cancelled(State) -> {ok, State}.
handoff_finished(_, State) -> {ok, State}.
handle_handoff_command(_Message, _Sender, State) -> {noreply, State}.
handle_handoff_data(_Arg0, _Arg1) -> erlang:error(not_implemented).
encode_handoff_item(_ObjectName, _ObjectValue) -> <<>>.
is_empty(State) -> {true, State}.
handle_exit(_Pid, _Reason, State) -> {noreply, State}.
terminate(_Reason, _State) -> ok.
delete(State) -> {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% Internal Functions

-spec get_mrvc(cache_id()) -> pvc_vc().
get_mrvc(StateTable) ->
    ets:lookup_element(StateTable, mrvc, 2).

-spec update_mrvc(cache_id(), pvc_vc()) -> ok.
update_mrvc(PartitionState, MRVC) ->
    true = ets:update_element(PartitionState, mrvc, {2, MRVC}),
    ok.

-spec internal_read_key(key(), pvc_vc(), cache_id()) -> {val(), pvc_vc()}.
internal_read_key(Key, MinVC, Storage) ->
    case ets:lookup(Storage, Key) of
        [] ->
            {<<>>, pvc_vclock:new()};
        [{Key, PrevVersionLog}] ->
            pvc_version_log:get_smaller(MinVC, PrevVersionLog)
    end.

-spec internal_read_range(key(), pvc_indices:range(), non_neg_integer(), cache_id()) -> [key()].
internal_read_range(Root, Range, Limit, RangeStorage) ->
    case ets:lookup(RangeStorage, Root) of
        [] -> [];
        [{Root, _}] ->
            Next = ets:next(RangeStorage, Root),
            internal_read_range(Next, Range, Limit, RangeStorage, [])
    end.

-spec internal_read_range(key(), pvc_indices:range(), non_neg_integer(), cache_id(), [key()]) -> [key()].
internal_read_range('$end_of_table', _Range, _Limit, _RangeStorage, Acc) ->
    %% We have reached the end of the table, return
    Acc;

internal_read_range(_Key, _Range, 0, _RangeStorage, Acc) ->
    %% We have hit the limit, return
    Acc;

internal_read_range(Key, Range, Limit, RangeStorage, Acc) ->
    case pvc_indices:in_range(Key, Range) of
        true ->
            Next = ets:next(RangeStorage, Key),
            internal_read_range(Next, Range, Limit - 1, RangeStorage, [Key | Acc]);
        false ->
            %% We are past the useful range, return the accumulator
            Acc
    end.

-spec ready(partition_id(), atom()) -> boolean().
ready(Partition, Name) ->
    case ets:info(get_cache_name(Partition, Name)) of
        undefined -> false;
        _ -> true
    end.

-spec prepare_internal(txid(), pvc_fsm:ws(), pvc_vc(), #state{}) -> {_, #state{}}.
prepare_internal(TxId, WriteSet, PrepareVC, State = #state{partition=Partition,
                                                          last_committed_version_cache = CommittedCache,
                                                          partition_state = PartitionState,
                                                          commit_queue = CommitQueue}) ->

    WriteSetDisputed = pvc_commit_queue:contains_disputed(WriteSet, CommitQueue),
    StaleTransaction = are_keys_stale(Partition, WriteSet, PrepareVC, CommittedCache),
    {Vote, NewsTate} = case WriteSetDisputed orelse StaleTransaction of
        true ->
            Reason = case WriteSetDisputed of
                true -> pvc_conflict;
                false -> pvc_stale_vc
            end,
            {{error, Reason}, State};
        false ->
            SeqNumber = incr_last_prepared(PartitionState),
            NewCommitQueue = pvc_commit_queue:enqueue(TxId, WriteSet, CommitQueue),
            {{ok, SeqNumber}, State#state{commit_queue=NewCommitQueue}}
    end,
    VoteMsg = {vote, Partition, Vote},
    {VoteMsg, NewsTate}.

-spec are_keys_stale(partition_id(), pvc_fsm:ws(), pvc_vc(), cache_id()) -> boolean().
are_keys_stale(_, [], _, _) ->
    false;

are_keys_stale(SelfPartition, [{Key, _} | Rest], PrepareVC, CommittedCache) ->
    case ets:lookup(CommittedCache, Key) of
        [] ->
            are_keys_stale(SelfPartition, Rest, PrepareVC, CommittedCache);

        [{Key, CommitTime}] ->
            PrepareTime = pvc_vclock:get_time(SelfPartition, PrepareVC),
            CommitTime > PrepareTime
    end.

-spec incr_last_prepared(cache_id()) -> non_neg_integer().
incr_last_prepared(PartitionState) ->
    ets:update_counter(PartitionState, seq_number, 1).

decide_internal(TxId, IndexKeySet, Outcome, State=#state{partition=Partition,
                                                         commit_queue=CommitQueue}) ->

    NewQueue = case Outcome of
        {error, _} ->
            pvc_commit_queue:remove(TxId, CommitQueue);
        {ok, CommitVC} ->
            ReadyQueue = pvc_commit_queue:ready(TxId, IndexKeySet, CommitVC, CommitQueue),
            ok = schedule_queue(Partition),
            ReadyQueue
    end,
    State#state{commit_queue=NewQueue}.

-spec process_queue_internal(#state{}) -> #state{}.
process_queue_internal(State=#state{partition=Partition,
                                    clog=CLog,
                                    storage=Storage,
                                    range_storage=RangeStorage,
                                    commit_queue=CommitQueue,
                                    partition_state=PartitionState,
                                    last_committed_version_cache=CommittedCache}) ->

    {ReadyTx, NewQueue} = pvc_commit_queue:dequeue_ready(CommitQueue),
    case ReadyTx of
        [] ->
            State#state{commit_queue=NewQueue};

        Entries ->
            NewCLog = lists:foldl(fun({_TxId, WS, VC, IndexKeySet}, AccClog) ->
                %% First, apply update to the VLog
                ok = vlog_apply_updates(Partition, WS, VC, Storage),

                %% Now, update the range storage
                ok = update_index_range(IndexKeySet, RangeStorage),

                %% Now, update MRVC with the max of the old value and our VC
                PrevMRVC = get_mrvc(PartitionState),
                MRVC = pvc_vclock:max(VC, PrevMRVC),
                ok = update_mrvc(PartitionState, MRVC),

                %% Store commits, update CLog
                NewCLog = pvc_commit_log:insert(MRVC, AccClog),

                %% Cache last commit time for the WS keys (for stale VC check)
                ok = cache_last_committed_version(Partition, WS, VC, CommittedCache),

                NewCLog
            end, CLog, Entries),

            State#state{commit_queue=NewQueue, clog=NewCLog}
    end.

%% @doc Apply the updates in the writeset to this partition storage
%%
%%      All the keys in the writeset are from this partition.
%%
-spec vlog_apply_updates(partition_id(), pvc_fsm:ws(), pvc_vc(), cache_id()) -> ok.
vlog_apply_updates(_Partition, [], _VC, _Storage) ->
    ok;

vlog_apply_updates(Partition, [{Key, Value} | Rest], VC, Storage) ->
    VLog = case ets:lookup(Storage, Key) of
        [] -> pvc_version_log:new(Partition);
        [{Key, PrevVLog}] -> PrevVLog
    end,
    NewVLog = pvc_version_log:insert(VC, Value, VLog),
    true = ets:insert(Storage, {Key, NewVLog}),
    vlog_apply_updates(Partition, Rest, VC, Storage).

%% @doc Add the index key set to the ordered storage
%%
%%      All the keys in the set are from this partition
%%
-spec update_index_range([key()], cache_id()) -> ok.
update_index_range(IndexKeySet, RangeStorage) ->
    Objects = [{Key, nil} || Key <- IndexKeySet],
    true = ets:insert(RangeStorage, Objects),
    ok.

%% @doc Cache the last commit time of the keys in the writeset
-spec cache_last_committed_version(partition_id(), pvc_fsm:ws(), pvc_vc(), cache_id()) -> ok.
cache_last_committed_version(Partition, WS, CommitVC, CommittedCache) ->
    CommitTime = pvc_vclock:get_time(Partition, CommitVC),
    Objects = [{Key, CommitTime} || {Key, _} <- WS],
    true = ets:insert(CommittedCache, Objects),
    ok.
