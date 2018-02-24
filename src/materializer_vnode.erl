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
-module(materializer_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%% Number of snapshots to trigger GC
-define(SNAPSHOT_THRESHOLD, 10).
%% Number of snapshots to keep after GC
-define(SNAPSHOT_MIN, 3).
%% Number of ops to keep before GC
-define(OPS_THRESHOLD, 50).
%% If after the op GC there are only this many or less spaces
%% free in the op list then increase the list size
-define(RESIZE_THRESHOLD, 5).
%% Only store the new SS if the following number of ops
%% were applied to the previous SS
-define(MIN_OP_STORE_SS, 5).
%% Expected time to wait until the logging vnode is up
-define(LOG_STARTUP_WAIT, 1000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
         check_tables_ready/0,
         read/5,
         get_cache_name/2,
         store_ss/3,
         update/2,
         tuple_to_key/2,
         belongs_to_snapshot_op/3]).

%% Callbacks
-export([init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

%% PVC-only functions
-export([pvc_read/5,
         pvc_update/1]).

-type op_and_id() :: {non_neg_integer(), #clocksi_payload{}}.

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time, this does not touch the vnode process
%%      directly, instead it just reads from the operations and snapshot tables that
%%      are in shared memory, allowing concurrent reads.
-spec read(key(), type(), snapshot_time(), #transaction{}, #mat_state{}) -> {ok, snapshot()} | {error, reason()}.
read(Key, Type, SnapshotTime, Transaction, MatState = #mat_state{ops_cache = OpsCache}) ->
    case ets:info(OpsCache) of
        undefined ->
            riak_core_vnode_master:sync_command(
                {MatState#mat_state.partition, node()},
                {read, Key, Type, SnapshotTime, Transaction},
                materializer_vnode_master,
                infinity
            );

        _ ->
            TxId = Transaction#transaction.txn_id,
            internal_read(Key, Type, SnapshotTime, TxId, MatState)
    end.

%% @doc Same as read/5, but return also the commit time of the snapshot.
%%
%%      Will also bypass the ops cache as we don't need it.
%%
-spec pvc_read(pvc, key(), type(), snapshot_time(), #mat_state{}) -> {ok, snapshot(), snapshot_time()} | {error, reason()}.
pvc_read(pvc, Key, Type, SnapshotTime, MatState = #mat_state{pvc_vlog_cache = VLogCache}) ->
    case ets:info(VLogCache) of
        undefined ->
            riak_core_vnode_master:sync_command(
                {MatState#mat_state.partition, node()},
                {pvc_read, Key, Type, SnapshotTime},
                materializer_vnode_master,
                infinity
            );

        _ ->
            pvc_internal_read(Key, Type, SnapshotTime, MatState)
    end.

-spec get_cache_name(non_neg_integer(), atom()) -> atom().
get_cache_name(Partition, Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).

%%@doc write operation to cache for future read, updates are stored
%%     one at a time into the ets tables
-spec update(key(), clocksi_payload()) -> ok | {error, reason()}.
update(Key, DownstreamOp) ->
    IndexNode = log_utilities:get_key_partition(Key),
    riak_core_vnode_master:sync_command(
        IndexNode,
        {update, Key, DownstreamOp},
        materializer_vnode_master
    ).

-spec pvc_update(clocksi_payload()) -> ok | {error, reason()}.
pvc_update(Payload = #clocksi_payload{key = Key}) ->
    IndexNode = log_utilities:get_key_partition(Key),
    riak_core_vnode_master:sync_command(
        IndexNode,
        {pvc_update, Payload},
        materializer_vnode_master
    ).

%%@doc write snapshot to cache for future read, snapshots are stored
%%     one at a time into the ets table
-spec store_ss(key(), #materialized_snapshot{}, snapshot_time()) -> ok.
store_ss(Key, Snapshot, CommitTime) ->
    IndexNode = log_utilities:get_key_partition(Key),
    riak_core_vnode_master:command(
        IndexNode,
        {store_ss, Key, Snapshot, CommitTime},
        materializer_vnode_master
    ).

init([Partition]) ->
    OpsCache = open_table(Partition, ops_cache),
    SnapshotCache = open_table(Partition, snapshot_cache),
    IsReady = case application:get_env(antidote, recover_from_log) of
        {ok, true} ->
            lager:debug("Checking for logs to init materializer ~p", [Partition]),
            riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
            false;

        _ ->
            true
    end,

    PVC_VLog = case antidote_config:get(?TRANSACTION_CONFIG, clocksi) of
        {ok, pvc} ->
            open_table(Partition, pvc_snapshot_cache);
        _ ->
            undefined
    end,

    PVC_Index = case antidote_config:get(?TRANSACTION_CONFIG, clocksi) of
        {ok, pvc} ->
            open_table(Partition, pvc_index_cache, [ordered_set, protected, named_table, ?TABLE_CONCURRENCY]);
        _ ->
            undefined
    end,

    {ok, #mat_state{
        is_ready = IsReady,
        ops_cache = OpsCache,
        partition = Partition,
        snapshot_cache = SnapshotCache,

        pvc_vlog_cache = PVC_VLog,
        pvc_index_set = PVC_Index
    }}.

-spec load_from_log_to_tables(partition_id(), #mat_state{}) -> ok | {error, reason()}.
load_from_log_to_tables(Partition, State) ->
    LogId = [Partition],
    Node = {Partition, log_utilities:get_my_node(Partition)},
    loop_until_loaded(Node, LogId, start, dict:new(), State).

-spec loop_until_loaded({partition_id(), node()}, log_id(), start | disk_log:continuation(), dict:dict(), #mat_state{}) -> ok | {error, reason()}.
loop_until_loaded(Node, LogId, Continuation, Ops, State) ->
    case logging_vnode:get_all(Node, LogId, Continuation, Ops) of
        {error, Reason} ->
            {error, Reason};
        {NewContinuation, NewOps, OpsDict} ->
            load_ops(OpsDict, State),
            loop_until_loaded(Node, LogId, NewContinuation, NewOps, State);
        {eof, OpsDict} ->
            load_ops(OpsDict, State),
            ok
    end.

-spec load_ops(dict:dict(), #mat_state{}) -> true.
load_ops(OpsDict, State) ->
    dict:fold(fun(Key, CommittedOps, _Acc) ->
        lists:foreach(fun({_OpId, Op}) ->
            #clocksi_payload{key = Key} = Op,
            op_insert_gc(Key, Op, State)
        end, CommittedOps)
    end, true, OpsDict).

-spec open_table(partition_id(), atom()) -> atom() | ets:tid().
open_table(Partition, Name) ->
    open_table(Partition, Name, [set, protected, named_table, ?TABLE_CONCURRENCY]).

open_table(Partition, Name, Options) ->
    case ets:info(get_cache_name(Partition, Name)) of
        undefined ->
            ets:new(get_cache_name(Partition, Name), Options);

        _ ->
            %% Other vnode hasn't finished closing tables
            lager:debug("Unable to open ets table in materializer vnode, retrying"),
            timer:sleep(100),
            try
                ets:delete(get_cache_name(Partition, Name))
            catch
                _:_Reason->
                    ok
            end,
            open_table(Partition, Name, Options)
    end.

%% @doc The tables holding the updates and snapshots are shared with concurrent
%%      readers, allowing them to be non-blocking and concurrent.
%%      This function checks whether or not all tables have been intialized or not yet.
%%      Returns true if the have, false otherwise.
-spec check_tables_ready() -> boolean().
check_tables_ready() ->
    PartitionList = dc_utilities:get_all_partitions_nodes(),
    check_table_ready(PartitionList).

-spec check_table_ready([{partition_id(), node()}]) -> boolean().
check_table_ready([]) ->
    true;
check_table_ready([{Partition, Node}|Rest]) ->
    Result =
        try
            riak_core_vnode_master:sync_command({Partition, Node},
                                                {check_ready},
                                                materializer_vnode_master,
                                                infinity)
        catch
            _:_Reason ->
                false
        end,
    case Result of
        true ->
            check_table_ready(Rest);
        false ->
            false
    end.

handle_command({hello}, _Sender, State) ->
  {reply, ok, State};

handle_command({check_ready}, _Sender, State = #mat_state{partition=Partition, is_ready=IsReady}) ->
    Result = case ets:info(get_cache_name(Partition, ops_cache)) of
        undefined ->
            false;

        _ ->
            case ets:info(get_cache_name(Partition, snapshot_cache)) of
                undefined ->
                    false;
                _ ->
                    true
            end
    end,
    Result2 = Result and IsReady,
    {reply, Result2, State};

handle_command({read, Key, Type, SnapshotTime, Transaction}, _Sender, State) ->
    {reply, read(Key, Type, SnapshotTime, Transaction, State), State};

handle_command({pvc_read, Key, Type, SnapshotTime}, _Sender, State) ->
    {reply, pvc_read(pvc, Key, Type, SnapshotTime, State), State};

handle_command({update, Key, DownstreamOp}, _Sender, State) ->
    true = op_insert_gc(Key, DownstreamOp, State),
    {reply, ok, State};

handle_command({pvc_update, Payload}, _Sender, State) ->
    ok = pvc_update_ops_bypass(Payload, State),
    {reply, ok, State};

handle_command({store_ss, Key, Snapshot, CommitTime}, _Sender, State) ->
    internal_store_ss(Key, Snapshot, CommitTime, false, State),
    {noreply, State};

handle_command(load_from_log, _Sender, State=#mat_state{partition=Partition}) ->
    IsReady = try
                case load_from_log_to_tables(Partition, State) of
                    ok ->
                        lager:debug("Finished loading from log to materializer on partition ~w", [Partition]),
                        true;
                    {error, not_ready} ->
                        false;
                    {error, Reason} ->
                        lager:error("Unable to load logs from disk: ~w, continuing", [Reason]),
                        true
                end
            catch
                _:Reason1 ->
                    lager:debug("Error loading from log ~w, will retry", [Reason1]),
                    false
            end,
    ok = case IsReady of
        false ->
            riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
            ok;
        true ->
            ok
    end,
    {noreply, State#mat_state{is_ready=IsReady}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0},
                       _Sender,
                       State = #mat_state{ops_cache = OpsCache}) ->
    F = fun(Key, A) ->
            [Key1|_] = tuple_to_list(Key),
            Fun(Key1, Key, A)
        end,
    Acc = ets:foldl(F, Acc0, OpsCache),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State=#mat_state{ops_cache=OpsCache}) ->
    {_Key, Operation} = binary_to_term(Data),
    true = ets:insert(OpsCache, Operation),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#mat_state{ops_cache=OpsCache}) ->
    case ets:first(OpsCache) of
        '$end_of_table' ->
            {true, State};
        _ ->
            {false, State}
    end.

delete(State=#mat_state{ops_cache=_OpsCache}) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State=#mat_state{
    ops_cache=OpsCache,
    snapshot_cache=SnapshotCache
}) ->
    try
        ets:delete(OpsCache),
        ets:delete(SnapshotCache)
    catch
        _:_Reason->
            ok
    end,
    ok.


%%---------------- Internal Functions -------------------%%

-spec internal_store_ss(key(),
    #materialized_snapshot{},
    snapshot_time(),
    boolean(),
    #mat_state{}
) -> boolean().

internal_store_ss(Key, Snapshot, CommitTime, ShouldGc, State = #mat_state{
    snapshot_cache=SnapshotCache
}) ->

    SnapshotDict = case ets:lookup(SnapshotCache, Key) of
        [] ->
            vector_orddict:new();

        [{_, SnapshotDictA}] ->
            SnapshotDictA
    end,
    %% Check if this snapshot is newer than the ones already in the cache. Since reads are concurrent multiple
    %% insert requests for the same snapshot could have occured
    ShouldInsert = case vector_orddict:size(SnapshotDict) > 0 of
        false ->
            true;

        true ->
            #materialized_snapshot{last_op_id = NewOpId} = Snapshot,
            {_Vector, FirstSnapshot} = vector_orddict:first(SnapshotDict),
            ((NewOpId - FirstSnapshot#materialized_snapshot.last_op_id) >= ?MIN_OP_STORE_SS)
    end,
    case (ShouldInsert or ShouldGc) of
        true ->
            SnapshotDict1 = vector_orddict:insert_bigger(CommitTime, Snapshot, SnapshotDict),
            snapshot_insert_gc(Key, SnapshotDict1, ShouldGc, State);
        false ->
            false
    end.

%% @doc Simplified read for pvc, bypass the materializer and ops cache step
-spec pvc_internal_read(key(), type(), snapshot_time(), #mat_state{}) -> {ok, term(), snapshot_time()}.
pvc_internal_read(Key, Type=antidote_crdt_lwwreg, MinSnapshotTime, #mat_state{
    pvc_vlog_cache = VLogCache
}) ->
    {Val, CommitVC} = case ets:lookup(VLogCache, Key) of
        [] ->
            BaseValue = Type:value(Type:new()),
            {BaseValue, vectorclock:new()};

        [{_, PrevVersionLog}] ->
            pvc_version_log:get_smaller(MinSnapshotTime, PrevVersionLog)
    end,
    {ok, Val, CommitVC}.

%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
-spec internal_read(key(), type(), snapshot_time(), txid() | ignore, #mat_state{}) -> {ok, snapshot()} | {error, reason()}.
internal_read(Key, Type, MinSnapshotTime, TxId, State) ->
    internal_read(Key, Type, MinSnapshotTime, TxId, false, State).

internal_read(Key, Type, MinSnapshotTime, TxId, ShouldGc, State) ->
    %% First get the most recent snapshot older than the specified time
    SnapshotGetResp = get_from_snapshot_cache(TxId, Key, Type, MinSnapshotTime, State),

    %% Now apply the operations to the snapshot, and return a materialized value
    materialize_snapshot(TxId, Key, Type, MinSnapshotTime, ShouldGc, State, SnapshotGetResp).

%% @doc Get the most recent snapshot from the cache (smaller thant the given commit time) for a given key.
%%
%%      If there's no in-memory suitable snapshot, it will fetch it from the replication log.
%%
-spec get_from_snapshot_cache(
    TxId :: txid() | ignore,
    Key :: key(),
    Type :: type(),
    MinSnaphsotTime :: snapshot_time(),
    State :: #mat_state{}
) -> #snapshot_get_response{}.

get_from_snapshot_cache(TxId, Key, Type, MinSnaphsotTime, State = #mat_state{
    ops_cache=OpsCache,
    snapshot_cache=SnapshotCache
}) ->
    case ets:lookup(SnapshotCache, Key) of
        [] ->
            EmptySnapshot = #materialized_snapshot{
                last_op_id=0,
                value=clocksi_materializer:new(Type)
            },
            store_snapshot(TxId, Key, EmptySnapshot, vectorclock:new(), false, State),
            %% Create a base version committed at time ignore, i.e. bottom
            BaseVersion = {{ignore, EmptySnapshot}, true},
            update_snapshot_from_cache(BaseVersion, Key, OpsCache);

        [{_, SnapshotDict}] ->
            case vector_orddict:get_smaller(MinSnaphsotTime, SnapshotDict) of
                undefined ->
                    %% No in-memory snapshot, get it from replication log
                    get_from_snapshot_log(Key, Type, MinSnaphsotTime);

                FoundSnapshot ->
                    %% Snapshot was present, now update it with the operations found in the cache.
                    %%
                    %% Operations are taken from the in-memory cache.
                    %% Any snapshot already in the cache will have more recent operations
                    %% also in the cache, so no need to hit the log.
                    update_snapshot_from_cache(FoundSnapshot, Key, OpsCache)
            end
    end.

-spec get_from_snapshot_log(key(), type(), snapshot_time()) -> #snapshot_get_response{}.
get_from_snapshot_log(Key, Type, SnapshotTime) ->
    LogId = log_utilities:get_logid_from_key(Key),
    Partition = log_utilities:get_key_partition(Key),
    logging_vnode:get(Partition, LogId, SnapshotTime, Type, Key).

%% @doc Store a new key snapshot in the in-memory cache at the given commit time.
%%
%%      If `ShouldGC` is true, it will try to prune the in-memory cache before inserting.
%%
-spec store_snapshot(
    TxId :: txid() | ignore,
    Key :: key(),
    Snapshot :: #materialized_snapshot{},
    Time :: snapshot_time(),
    ShouldGC :: boolean(),
    MatState :: #mat_state{}
) -> ok.

store_snapshot(TxId, Key, Snapshot, Time, ShouldGC, MatState) ->
    case TxId of
        ignore ->
            internal_store_ss(Key, Snapshot, Time, ShouldGC, MatState),
            ok;
        _ ->
            materializer_vnode:store_ss(Key, Snapshot, Time)
    end.

%% @doc Given a snapshot from the cache, update it from the ops cache.
-spec update_snapshot_from_cache(
    {{snapshot_time() | ignore, #materialized_snapshot{}}, boolean()},
    key(),
    cache_id()
) -> #snapshot_get_response{}.

update_snapshot_from_cache(Snapshot, Key, OpsCache) ->
    {{SnapshotCommitTime, LatestSnapshot}, IsFirst} = Snapshot,
    {Ops, OpsLen} = fetch_updates_from_cache(OpsCache, Key),
    #snapshot_get_response{
        ops_list=Ops,
        number_of_ops=OpsLen,
        is_newest_snapshot=IsFirst,
        snapshot_time=SnapshotCommitTime,
        materialized_snapshot=LatestSnapshot
    }.

%% @doc Given a key, get all the operations in the ops cache.
%%
%%      Will also return how many operations were in the cache.
%%
-spec fetch_updates_from_cache(cache_id(), key()) -> {[op_and_id()] | tuple(), integer()}.
fetch_updates_from_cache(OpsCache, Key) ->
    case ets:lookup(OpsCache, Key) of
        [] ->
            {[], 0};

        [Tuple] ->
            {Key, Length, CachedOps} = tuple_to_cached_ops(Tuple),
            {CachedOps, Length}
    end.

-spec materialize_snapshot(
    txid() | ignore,
    key(),
    type(),
    snapshot_time(),
    boolean(),
    #mat_state{},
    #snapshot_get_response{}
) -> {ok, snapshot_time()} | {error, reason()}.

materialize_snapshot(_TxId, _Key, _Type, _SnapshotTime, _ShouldGC, _MatState, #snapshot_get_response{
    number_of_ops=0,
    materialized_snapshot=Snapshot
}) ->
    {ok, Snapshot#materialized_snapshot.value};

materialize_snapshot(TxId, Key, Type, SnapshotTime, ShouldGC, MatState, SnapshotResponse = #snapshot_get_response{
    is_newest_snapshot=IsNewest
}) ->
    case clocksi_materializer:materialize(Type, TxId, SnapshotTime, SnapshotResponse) of
        {error, Reason} ->
            {error, Reason};

        {ok, MaterializedSnapshot, NewLastOp, CommitTime, WasUpdated, OpsAdded} ->
            %% the following checks for the case there were no snapshots and there were operations,
            %% but none was applicable for the given snapshot_time
            %% But is the snapshot not safe?
            case CommitTime of
                ignore ->
                    {ok, MaterializedSnapshot};

                _ ->
                    %% Check if we need to refresh the cache
                    SufficientOps = OpsAdded >= ?MIN_OP_STORE_SS,
                    ShouldRefreshCache = WasUpdated and IsNewest and SufficientOps,

                    %% Only store the snapshot if it would be at the end of the list and
                    %% has new operations added to the previous snapshot
                    ok = case ShouldRefreshCache orelse ShouldGC of
                        false ->
                            ok;

                        true ->
                            ToCache = #materialized_snapshot{
                                last_op_id=NewLastOp,
                                value=MaterializedSnapshot
                            },
                            store_snapshot(TxId, Key, ToCache, CommitTime, ShouldGC, MatState)
                     end,
                    {ok, MaterializedSnapshot}
            end
    end.

%% Should be called doesn't belong in SS
%% returns true if op is more recent than SS (i.e. is not in the ss)
%% returns false otw
-spec belongs_to_snapshot_op(snapshot_time() | ignore, dc_and_commit_time(), snapshot_time()) -> boolean().
belongs_to_snapshot_op(ignore, {_OpDc, _OpCommitTime}, _OpSs) ->
    true;
belongs_to_snapshot_op(SSTime, {OpDc, OpCommitTime}, OpSs) ->
    OpSs1 = dict:store(OpDc, OpCommitTime, OpSs),
    not vectorclock:le(OpSs1, SSTime).

%% @doc Operation to insert a Snapshot in the cache and start
%%      Garbage collection triggered by reads.
-spec snapshot_insert_gc(key(),
    vector_orddict:vector_orddict(),
    boolean(),
    #mat_state{}
) -> true.

snapshot_insert_gc(Key, SnapshotDict, ShouldGc, #mat_state{
    ops_cache = OpsCache,
    snapshot_cache = SnapshotCache
}) ->

    %% Perform the garbage collection when the size of the snapshot dict passed the threshold
    %% or when a GC is forced (a GC is forced after every ?OPS_THRESHOLD ops are inserted into the cache)
    %% Should check op size here also, when run from op gc
    OverThreshold = vector_orddict:size(SnapshotDict) >= ?SNAPSHOT_THRESHOLD,
    case OverThreshold orelse ShouldGc of
        false ->
            true = ets:insert(SnapshotCache, {Key, SnapshotDict});

        true ->
            %% snapshots are no longer totally ordered
            PrunedSnapshots = vector_orddict:sublist(SnapshotDict, 1, ?SNAPSHOT_MIN),
            {CT, _S} = vector_orddict:last(PrunedSnapshots),
            CommitTime = lists:foldl(fun({CT1, _ST}, Acc) ->
                vectorclock:min([CT1, Acc])
            end, CT, vector_orddict:to_list(PrunedSnapshots)),

            {Key, Length, OpId, ListLen, OpsDict} = case ets:lookup(OpsCache, Key) of
                [] ->
                    {Key, 0, 0, 0, {}};
                [Tuple] ->
                    tuple_to_key(Tuple)
            end,
            {NewLength, PrunedOps} = prune_ops({Length, OpsDict}, CommitTime),
            true = ets:insert(SnapshotCache, {Key, PrunedSnapshots}),

            %% Check if the pruned ops are larger or smaller than the previous list size
            %% if so create a larger or smaller list (by dividing or multiplying by 2)
            %% (Another option would be to shrink to a more "minimum" size, but need to test to see what is better)
            NewListLen = case NewLength > ListLen - ?RESIZE_THRESHOLD of
                true ->
                    ListLen * 2;
                false ->
                    HalfListLen = ListLen div 2,
                    case HalfListLen =< ?OPS_THRESHOLD of
                        true ->
                            %% Don't shrink list, already minimun size
                            ListLen;
                        false ->
                            %% Only shrink if shrinking would leave some space for new ops
                            case HalfListLen - ?RESIZE_THRESHOLD > NewLength of
                                true ->
                                    HalfListLen;
                                false ->
                                    ListLen
                            end
                    end
            end,
            NewTuple = erlang:make_tuple(
                ?FIRST_OP + NewListLen,
                0,
                [{1, Key}, {2, {NewLength, NewListLen}}, {3, OpId} | PrunedOps]
            ),
            true = ets:insert(OpsCache, NewTuple)
    end.

%% @doc Remove from OpsDict all operations that have committed before Threshold.
-spec prune_ops(
    {non_neg_integer(), tuple()},
    snapshot_time()
) -> {non_neg_integer(), [{non_neg_integer(), op_and_id()}]}.
prune_ops({Len, OpsTuple}, Threshold)->
    %% should write custom function for this in the vector_orddict
    %% or have to just traverse the entire list?
    %% since the list is ordered, can just stop when all values of
    %% the op is smaller (i.e. not concurrent)
    %% So can add a stop function to ordered_filter
    %% Or can have the filter function return a tuple, one vale for stopping
    %% one for including
    {NewSize, NewOps} = check_filter(fun({_OpId, Op}) ->
        OpCommitTime=Op#clocksi_payload.commit_time,
        belongs_to_snapshot_op(Threshold, OpCommitTime, Op#clocksi_payload.snapshot_time)
    end, ?FIRST_OP, ?FIRST_OP + Len, ?FIRST_OP, OpsTuple, 0, []),
    case NewSize of
        0 ->
            First = element(?FIRST_OP + Len, OpsTuple),
            {1, [{?FIRST_OP, First}]};

        _ ->
            {NewSize, NewOps}
    end.

%% This function will go through a tuple of operations, filtering out the operations
%% that are out of date (given by the input function Fun), and returning a list
%% of the remaining operations and the size of that list
%% It is used during garbage collection to filter out operations that are older than any
%% of the cached snapshots
-spec check_filter(
    fun(({non_neg_integer(), #clocksi_payload{}}) -> boolean()),
    non_neg_integer(),
    non_neg_integer(),
    non_neg_integer(),
    tuple(),
    non_neg_integer(),
    [{non_neg_integer(), op_and_id()}]
) -> {non_neg_integer(), [{non_neg_integer(), op_and_id()}]}.

check_filter(_Fun, Id, Last, _NewId, _Tuple, NewSize, NewOps) when (Id == Last) ->
    {NewSize, NewOps};
check_filter(Fun, Id, Last, NewId, Tuple, NewSize, NewOps) ->
    Op = element(Id, Tuple),
    case Fun(Op) of
        true ->
            check_filter(Fun, Id+1, Last, NewId+1, Tuple, NewSize+1, [{NewId, Op}|NewOps]);
        false ->
            check_filter(Fun, Id+1, Last, NewId, Tuple, NewSize, NewOps)
    end.

%% @doc Transform a key into the internal represntation used by the ops cache
-spec key_to_tuple(key()) -> tuple().
key_to_tuple(Key) ->
    erlang:make_tuple(
        ?FIRST_OP + ?OPS_THRESHOLD,
        0,
        [{1, Key}, {2, {0, ?OPS_THRESHOLD}}]
    ).

-spec tuple_to_key(tuple()) -> {key(), integer(), non_neg_integer(), non_neg_integer(), tuple()}.
tuple_to_key(Tuple) ->
    tuple_to_key(Tuple, false).

%% This is an internal function used to convert the tuple stored in ets
%% to a tuple and list usable by the materializer
%% The second argument if true will convert the ops tuple to a list of ops
%% Otherwise it will be kept as a tuple
-spec tuple_to_key(
    tuple(),
    boolean()
) -> {any(), integer(), non_neg_integer(), non_neg_integer(), [op_and_id()] | tuple()}.

tuple_to_key(Tuple, ToList) ->
    Key = element(1, Tuple),
    {Length, ListLen} = element(2, Tuple),
    OpId = element(3, Tuple),
    Ops = case ToList of
        false ->
            Tuple;
        true ->
            tuple_to_key_int(?FIRST_OP, Length+?FIRST_OP, Tuple, [])
    end,
    {Key, Length, OpId, ListLen, Ops}.

%% FIXME(borja): Broken since this will always match and return the empty list
tuple_to_key_int(Next, Next, _Tuple, Acc) ->
    Acc;

tuple_to_key_int(Next, Last, Tuple, Acc) ->
    tuple_to_key_int(Next+1, Last, Tuple, [element(Next, Tuple)|Acc]).

-spec tuple_to_cached_ops(tuple()) -> {key(), integer(), tuple()}.
tuple_to_cached_ops(Tuple) ->
    Key = element(1, Tuple),
    {Length, _} = element(2, Tuple),
    Ops = Tuple,
    {Key, Length, Ops}.

%% @doc Simplified materializer update for pvc
%%
%%      Instead of storing ops and applying those to the previous
%%      snapshot on read, materialize that and store it back,
%%      bypass the ops cache and create a new snapshot, given that
%%      we only operate on lww registers, the update is the same
%%      as the value.
%%
%% TODO(borja): Will ignore transactions from other DCs
%% By ignoring the ops cache, it will miss updates from other
%% data centers. Right now when a transaction is delivered from
%% another DC, Antidote will store the updates in the operation
%% cache of the materializer. It will also store the operations
%% in the log, so those operations will be there when we perform
%% the CLog scan (see pvc_find_maxvc/2 in clocksi_readitem_server)
%%
-spec pvc_update_ops_bypass(clocksi_payload(), #mat_state{}) -> ok.
pvc_update_ops_bypass(Payload, #mat_state{partition = Partition,
                                          pvc_vlog_cache = VLogCache}) ->

    #clocksi_payload{key = Key,
                     type = Type,
                     op_param = DownstreamOp,
                     snapshot_time = SnapshotTime} = Payload,

    VersionLog = case ets:lookup(VLogCache, Key) of
        [] ->
            pvc_version_log:new(Partition, Type);

        [{_, PrevVersionLog}] ->
            PrevVersionLog
    end,

    %% TODO(borja): Implement GC in the future
    NextVersionLog = pvc_version_log:insert(SnapshotTime, DownstreamOp, VersionLog),
%%    lager:info("VLog.apply(~p, ~p)", [Key, pvc_version_log:to_list(NextVersionLog)]),
    true = ets:insert(VLogCache, {Key, NextVersionLog}),
    ok.

%% @doc Insert an operation and start garbage collection triggered by writes.
%% the mechanism is very simple; when there are more than OPS_THRESHOLD
%% operations for a given key, just perform a read, that will trigger
%% the GC mechanism.
-spec op_insert_gc(key(), clocksi_payload(), #mat_state{}) -> true.
op_insert_gc(Key, DownstreamOp, State = #mat_state{ops_cache = OpsCache})->
    ok = case ets:member(OpsCache, Key) of
        true ->
            ok;

        false ->
            Tuple = key_to_tuple(Key),
            true = ets:insert(OpsCache, Tuple),
            ok
    end,
    NewId = faa_next_op_id(OpsCache, Key),
    {CurrSize, MaxSize} = get_ops_size(OpsCache, Key),
    %% Perform the GC incase the list is full, or every ?OPS_THRESHOLD operations (which ever comes first)
    case (CurrSize >= MaxSize) or ((NewId rem ?OPS_THRESHOLD) == 0) of
        false ->
            true = update_key_ops(OpsCache, Key, NewId, DownstreamOp, CurrSize, MaxSize);

        true ->
            ok = perform_gc(Key, DownstreamOp, State),
            true = update_key_ops(OpsCache, Key, NewId, DownstreamOp)
    end.

%% @doc Store a new clocksi payload with the given id under the given key
-spec update_key_ops(cache_id(), key(), non_neg_integer(), clocksi_payload()) -> boolean().
update_key_ops(OpsCache, Key, NewId, Op) ->
    {Size, MaxSize} = get_ops_size(OpsCache, Key),
    update_key_ops(OpsCache, Key, NewId, Op, Size, MaxSize).

%% @doc Same as update_key_ops/4, but leak internals about the key structure
%%
%%      Should supply the current and maximum size of the key representation
%%      in the snapshot cache.
%%
-spec update_key_ops(
    OpsCache :: cache_id(),
    Key :: key(),
    NewId :: non_neg_integer(),
    Op :: clocksi_payload(),
    Size :: non_neg_integer(),
    MaxSize :: non_neg_integer()
) -> boolean().

update_key_ops(OpsCache, Key, NewId, Op, Size, MaxSize) ->
    EtsOp = [
        {?FIRST_OP + Size, {NewId, Op}},
        {2, {Size + 1, MaxSize}}
    ],
    ets:update_element(OpsCache, Key, EtsOp).

%% @doc Fetch and add to the next id to be used for clocksi payloads
-spec faa_next_op_id(cache_id(), key()) -> non_neg_integer().
faa_next_op_id(OpsCache, Key) ->
    ets:update_counter(OpsCache, Key, {3, 1}).

%% @doc Get the current (and max) size of the ops list under this key.
-spec get_ops_size(cache_id(), key()) -> {non_neg_integer(), non_neg_integer()}.
get_ops_size(OpsCache, Key) ->
    ets:lookup_element(OpsCache, Key, 2).

%% @doc Garbage collect the snapshots associated with the given key.
-spec perform_gc(key(), clocksi_payload(), #mat_state{}) -> ok.
perform_gc(Key, #clocksi_payload{type=Type, snapshot_time=SSTime}, State) ->
    %% TODO: Extract GC implementation outside of internal read and use that
    %% with the 5th argument being "true", GC is performed by the internal read
    {_, _} = internal_read(Key, Type, SSTime, ignore, true, State),
    ok.

-ifdef(TEST).

serialize_key_test() ->
    Key = key,

    Ser = key_to_tuple(Key),
    DeserOps = tuple_to_cached_ops(Ser),

    ?assertMatch({Key, 0, Ser}, DeserOps),

    Deser = tuple_to_key(Ser, false),
    DeserList = tuple_to_key(Ser, true),

    ?assertMatch({Key, 0, 0, 50, Ser}, Deser),
    ?assertMatch({Key, 0, 0, 50, []}, DeserList).

%% @doc Testing belongs_to_snapshot returns true when a commit time
%% is smaller than a snapshot time
belongs_to_snapshot_test()->
    CommitTime1a= 1,
    CommitTime2a= 1,
    CommitTime1b= 1,
    CommitTime2b= 7,
    SnapshotClockDC1 = 5,
    SnapshotClockDC2 = 5,
    CommitTime3a= 5,
    CommitTime4a= 5,
    CommitTime3b= 10,
    CommitTime4b= 10,

    SnapshotVC=vectorclock:from_list([{1, SnapshotClockDC1}, {2, SnapshotClockDC2}]),
    ?assertEqual(true, belongs_to_snapshot_op(
                 vectorclock:from_list([{1, CommitTime1a}, {2, CommitTime1b}]), {1, SnapshotClockDC1}, SnapshotVC)),
    ?assertEqual(true, belongs_to_snapshot_op(
                 vectorclock:from_list([{1, CommitTime2a}, {2, CommitTime2b}]), {2, SnapshotClockDC2}, SnapshotVC)),
    ?assertEqual(false, belongs_to_snapshot_op(
                  vectorclock:from_list([{1, CommitTime3a}, {2, CommitTime3b}]), {1, SnapshotClockDC1}, SnapshotVC)),
    ?assertEqual(false, belongs_to_snapshot_op(
                  vectorclock:from_list([{1, CommitTime4a}, {2, CommitTime4b}]), {2, SnapshotClockDC2}, SnapshotVC)).

%% @doc This tests to make sure when garbage collection happens, no updates are lost
gc_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    DC1 = 1,
    Type = antidote_crdt_counter,

    %% Make 10 snapshots
    MatState = #mat_state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    {ok, Res0} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2}]), ignore, MatState),
    ?assertEqual(0, Type:value(Res0)),

    op_insert_gc(Key, generate_payload(10, 11, Res0, a1), MatState),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 12}]), ignore, MatState),
    ?assertEqual(1, Type:value(Res1)),

    op_insert_gc(Key, generate_payload(20, 21, Res1, a2), MatState),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 22}]), ignore, MatState),
    ?assertEqual(2, Type:value(Res2)),

    op_insert_gc(Key, generate_payload(30, 31, Res2, a3), MatState),
    {ok, Res3} = internal_read(Key, Type, vectorclock:from_list([{DC1, 32}]), ignore, MatState),
    ?assertEqual(3, Type:value(Res3)),

    op_insert_gc(Key, generate_payload(40, 41, Res3, a4), MatState),
    {ok, Res4} = internal_read(Key, Type, vectorclock:from_list([{DC1, 42}]), ignore, MatState),
    ?assertEqual(4, Type:value(Res4)),

    op_insert_gc(Key, generate_payload(50, 51, Res4, a5), MatState),
    {ok, Res5} = internal_read(Key, Type, vectorclock:from_list([{DC1, 52}]), ignore, MatState),
    ?assertEqual(5, Type:value(Res5)),

    op_insert_gc(Key, generate_payload(60, 61, Res5, a6), MatState),
    {ok, Res6} = internal_read(Key, Type, vectorclock:from_list([{DC1, 62}]), ignore, MatState),
    ?assertEqual(6, Type:value(Res6)),

    op_insert_gc(Key, generate_payload(70, 71, Res6, a7), MatState),
    {ok, Res7} = internal_read(Key, Type, vectorclock:from_list([{DC1, 72}]), ignore, MatState),
    ?assertEqual(7, Type:value(Res7)),

    op_insert_gc(Key, generate_payload(80, 81, Res7, a8), MatState),
    {ok, Res8} = internal_read(Key, Type, vectorclock:from_list([{DC1, 82}]), ignore, MatState),
    ?assertEqual(8, Type:value(Res8)),

    op_insert_gc(Key, generate_payload(90, 91, Res8, a9), MatState),
    {ok, Res9} = internal_read(Key, Type, vectorclock:from_list([{DC1, 92}]), ignore, MatState),
    ?assertEqual(9, Type:value(Res9)),

    op_insert_gc(Key, generate_payload(100, 101, Res9, a10), MatState),

    %% Insert some new values

    op_insert_gc(Key, generate_payload(15, 111, Res1, a11), MatState),
    op_insert_gc(Key, generate_payload(16, 121, Res1, a12), MatState),

    %% Trigger the clean
    {ok, Res10} = internal_read(Key, Type, vectorclock:from_list([{DC1, 102}]), ignore, MatState),
    ?assertEqual(10, Type:value(Res10)),

    op_insert_gc(Key, generate_payload(102, 131, Res9, a13), MatState),

    %% Be sure you didn't loose any updates
    {ok, Res13} = internal_read(Key, Type, vectorclock:from_list([{DC1, 142}]), ignore, MatState),
    ?assertEqual(13, Type:value(Res13)).

%% @doc This tests to make sure operation lists can be large and resized
large_list_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    DC1 = 1,
    Type = antidote_crdt_counter,
    MatState = #mat_state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Make 1000 updates to grow the list, whithout generating a snapshot to perform the gc
    {ok, Res0} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2}]), ignore, MatState),
    ?assertEqual(0, Type:value(Res0)),

    lists:foreach(fun(Val) ->
                      op_insert_gc(Key, generate_payload(10, 11+Val, Res0, Val), MatState)
                  end, lists:seq(1, 1000)),

    {ok, Res1000} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2000}]), ignore, MatState),
    ?assertEqual(1000, Type:value(Res1000)),

    %% Now check everything is ok as the list shrinks from generating new snapshots
    lists:foreach(fun(Val) ->
                      op_insert_gc(Key, generate_payload(10+Val, 11+Val, Res0, Val), MatState),
                      {ok, Res} = internal_read(Key, Type, vectorclock:from_list([{DC1, 2000}]), ignore, MatState),
                      ?assertEqual(Val, Type:value(Res))
                  end, lists:seq(1001, 1100)).

generate_payload(SnapshotTime, CommitTime, Prev, _Name) ->
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = 1,

    {ok, Op1} = Type:downstream({increment, 1}, Prev),
    #clocksi_payload{key = Key,
                     type = Type,
                     op_param = Op1,
                     snapshot_time = vectorclock:from_list([{DC1, SnapshotTime}]),
                     commit_time = {DC1, CommitTime},
                     txid = 1
                    }.

seq_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = 1,
    S1 = Type:new(),
    MatState = #mat_state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Insert one increment
    {ok, Op1} = Type:downstream({increment, 1}, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = Op1,
                                     snapshot_time = vectorclock:from_list([{DC1, 10}]),
                                     commit_time = {DC1, 15},
                                     txid = 1
                                    },
    op_insert_gc(Key, DownstreamOp1, MatState),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}]), ignore, MatState),
    ?assertEqual(1, Type:value(Res1)),
    %% Insert second increment
    {ok, Op2} = Type:downstream({increment, 1}, S1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = Op2,
                      snapshot_time=vectorclock:from_list([{DC1, 16}]),
                      commit_time = {DC1, 20},
                      txid=2},

    op_insert_gc(Key, DownstreamOp2, MatState),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 21}]), ignore, MatState),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}]), ignore, MatState),
    ?assertEqual(1, Type:value(ReadOld)).

multipledc_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = 1,
    DC2 = 2,
    S1 = Type:new(),
    MatState = #mat_state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},


    %% Insert one increment in DC1
    {ok, Op1} = Type:downstream({increment, 1}, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = Op1,
                                     snapshot_time = vectorclock:from_list([{DC2, 0}, {DC1, 10}]),
                                     commit_time = {DC1, 15},
                                     txid = 1
                                    },
    op_insert_gc(Key, DownstreamOp1, MatState),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}, {DC2, 0}]), ignore, MatState),
    ?assertEqual(1, Type:value(Res1)),

    %% Insert second increment in other DC
    {ok, Op2} = Type:downstream({increment, 1}, S1),
    DownstreamOp2 = DownstreamOp1#clocksi_payload{
                      op_param = Op2,
                      snapshot_time=vectorclock:from_list([{DC2, 16}, {DC1, 16}]),
                      commit_time = {DC2, 20},
                      txid=2},

    op_insert_gc(Key, DownstreamOp2, MatState),
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 16}, {DC2, 21}]), ignore, MatState),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, ReadOld} = internal_read(Key, Type, vectorclock:from_list([{DC1, 15}, {DC2, 15}]), ignore, MatState),
    ?assertEqual(1, Type:value(ReadOld)).

concurrent_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = antidote_crdt_counter,
    DC1 = local,
    DC2 = remote,
    S1 = Type:new(),
    MatState = #mat_state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},

    %% Insert one increment in DC1
    {ok, Op1} = Type:downstream({increment, 1}, S1),
    DownstreamOp1 = #clocksi_payload{key = Key,
                                     type = Type,
                                     op_param = Op1,
                                     snapshot_time = vectorclock:from_list([{DC1, 0}, {DC2, 0}]),
                                     commit_time = {DC2, 1},
                                     txid = 1
                                    },
    op_insert_gc(Key, DownstreamOp1, MatState),
    {ok, Res1} = internal_read(Key, Type, vectorclock:from_list([{DC2, 1}, {DC1, 0}]), ignore, MatState),
    ?assertEqual(1, Type:value(Res1)),

    %% Another concurrent increment in other DC
    {ok, Op2} = Type:downstream({increment, 1}, S1),
    DownstreamOp2 = #clocksi_payload{ key = Key,
                                      type = Type,
                                      op_param = Op2,
                                      snapshot_time=vectorclock:from_list([{DC1, 0}, {DC2, 0}]),
                                      commit_time = {DC1, 1},
                                      txid=2},
    op_insert_gc(Key, DownstreamOp2, MatState),

    %% Read different snapshots
    {ok, ReadDC1} = internal_read(Key, Type, vectorclock:from_list([{DC1, 1}, {DC2, 0}]), ignore, MatState),
    ?assertEqual(1, Type:value(ReadDC1)),
    io:format("Result1 = ~p", [ReadDC1]),
    {ok, ReadDC2} = internal_read(Key, Type, vectorclock:from_list([{DC1, 0}, {DC2, 1}]), ignore, MatState),
    io:format("Result2 = ~p", [ReadDC2]),
    ?assertEqual(1, Type:value(ReadDC2)),

    %% Read snapshot including both increments
    {ok, Res2} = internal_read(Key, Type, vectorclock:from_list([{DC2, 1}, {DC1, 1}]), ignore, MatState),
    ?assertEqual(2, Type:value(Res2)).

%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%% E.g., for a gcounter, return 0.
read_nonexisting_key_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    MatState = #mat_state{ops_cache = OpsCache, snapshot_cache = SnapshotCache},
    Type = riak_dt_gcounter,
    {ok, ReadResult} = internal_read(key, Type, vectorclock:from_list([{dc1, 1}, {dc2, 0}]), ignore, MatState),
    ?assertEqual(0, Type:value(ReadResult)).

-endif.
