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
-module(clocksi_readitem_server).

-behavior(gen_server).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_link/2]).

%% Callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         code_change/3,
         handle_event/3,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).

%% States
-export([read_data_item/4,
         async_read_data_item/5,
         check_partition_ready/3,
         start_read_servers/2,
         stop_read_servers/2]).

%% PVC only
-export([pvc_async_read/3]).

%% TODO(borja): Remove
-export([pvc_find_maxvc/4]).

%% Spawn
-record(state, {
    self :: atom(),
    id :: non_neg_integer(),
    mat_state :: #mat_state{},
    partition :: partition_id(),
    prepared_cache :: cache_id(),

    %% PVC Atomic cache
    pvc_atomic_cache :: atom()
}).

%% TODO: allow properties for reads
%% -type external_read_property() :: {external_read, dcid(), dc_and_commit_time(), snapshot_time()}.
%% -type read_property() :: external_read_property().
%% -type read_property_list() :: [read_property()].

%%%===================================================================
%%% API
%%%===================================================================

%% @doc This starts a gen_server responsible for servicing reads to key
%%      handled by this Partition.  To allow for read concurrency there
%%      can be multiple copies of these servers per parition, the Id is
%%      used to distinguish between them.  Since these servers will be
%%      reading from ets tables shared by the clock_si and materializer
%%      vnodes, they should be started on the same physical nodes as
%%      the vnodes with the same partition.
-spec start_link(partition_id(), non_neg_integer()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Partition, Id) ->
    Addr = node(),
    gen_server:start_link({global, generate_server_name(Addr, Partition, Id)}, ?MODULE, [Partition, Id], []).

-spec start_read_servers(partition_id(), non_neg_integer()) -> ok.
start_read_servers(Partition, Count) ->
    start_read_servers_internal(node(), Partition, Count).

-spec stop_read_servers(partition_id(), non_neg_integer()) -> ok.
stop_read_servers(Partition, Count) ->
    stop_read_servers_internal(node(), Partition, Count).

-spec read_data_item(index_node(), key(), type(), tx()) -> {error, term()} | {ok, snapshot()}.
read_data_item({Partition, Node}=IdxNode, Key, Type, Transaction) ->
    try
        gen_server:call({global, generate_random_server_name(Node, Partition)},
                        {perform_read, Key, Type, Transaction}, infinity)
    catch
        _:Reason ->
            lager:debug("Exception caught: ~p, starting read server to fix", [Reason]),
            start_read_servers(IdxNode),
            read_data_item(IdxNode, Key, Type, Transaction)
    end.

-spec async_read_data_item(index_node(), key(), type(), tx(), term()) -> ok.
async_read_data_item({Partition, Node}, Key, Type, Transaction, Coordinator) ->
    gen_server:cast(
        {global, generate_random_server_name(Node, Partition)},
        {perform_read_cast, Coordinator, Key, Type, Transaction}
    ).

%% @doc PVC-only asynchronous read
-spec pvc_async_read(key(), sets:set(), pvc_vc()) -> ok.
pvc_async_read(Key, HasRead, VCaggr) ->
    %% If not read, go through wait process
    {Partition, Node}=IndexNode = log_utilities:get_key_partition(Key),
    Target = {global, generate_random_server_name(Node, Partition)},
    case sets:is_element(Partition, HasRead) of
        false ->
            gen_server:cast(Target, {pvc_fresh_read, self(), IndexNode, Key, HasRead, VCaggr});
        true ->
            %% If partition has been read, read directly from VLog
            gen_server:cast(Target, {pvc_vlog_read, self(), Key, VCaggr})
    end.

-spec start_read_servers(index_node()) -> boolean().
start_read_servers(IndexNode) ->
    try
        riak_core_vnode_master:sync_command(IndexNode,
                                            start_read_servers,
                                            ?CLOCKSI_MASTER,
                                            infinity)
    catch _:_Reason ->
        false
    end.

-spec check_partition_ready(node(), partition_id(), non_neg_integer()) -> boolean().
check_partition_ready(_Node, _Partition, 0) ->
    true;

check_partition_ready(Node, Partition, Num) ->
    case global:whereis_name(generate_server_name(Node, Partition, Num)) of
        undefined -> false;
        _ -> check_partition_ready(Node, Partition, Num - 1)
    end.

%%%===================================================================
%%% Internal
%%%===================================================================

-spec start_read_servers_internal(node(), partition_id(), non_neg_integer()) -> ok.
start_read_servers_internal(_Node, _Partition, 0) ->
    ok;

start_read_servers_internal(Node, Partition, Num) ->
    case clocksi_readitem_sup:start_fsm(Partition, Num) of
        {ok, _Id} ->
            start_read_servers_internal(Node, Partition, Num - 1);
        {error, {already_started, _}} ->
            start_read_servers_internal(Node, Partition, Num - 1);
        Err ->
            Name = generate_server_name(Node, Partition, Num),
            lager:debug("Unable to start clocksi read server for ~p, (reason ~p) will retry", [Name, Err]),
            try
                gen_server:call({global, Name}, go_down)
            catch
                _:_Reason->
                    ok
            end,
            start_read_servers_internal(Node, Partition, Num)
    end.

-spec stop_read_servers_internal(node(), partition_id(), non_neg_integer()) -> ok.
stop_read_servers_internal(_Node, _Partition, 0) ->
    ok;

stop_read_servers_internal(Node, Partition, Num) ->
    try
        gen_server:call({global, generate_server_name(Node, Partition, Num)}, go_down)
    catch
        _:_Reason->
           ok
    end,
    stop_read_servers_internal(Node, Partition, Num-1).

-spec generate_server_name(node(), partition_id(), non_neg_integer()) -> atom().
generate_server_name(Node, Partition, Id) ->
    BinId = integer_to_binary(Id),
    BinPart = integer_to_binary(Partition),
    BinNode = atom_to_binary(Node, latin1),
    Name = <<BinId/binary, <<"-">>/binary, BinPart/binary, <<"-">>/binary, BinNode/binary>>,
    case catch binary_to_existing_atom(Name, latin1) of
        {'EXIT', _} -> binary_to_atom(Name, latin1);
        Normal -> Normal
    end.

-spec generate_random_server_name(node(), partition_id()) -> atom().
generate_random_server_name(Node, Partition) ->
    generate_server_name(Node, Partition, rand_compat:uniform(?READ_CONCURRENCY)).

init([Partition, Id]) ->
    Addr = node(),

    %% Materializer Caches
    OpsCache = materializer_vnode:get_cache_name(Partition, ops_cache),
    SnapshotCache = materializer_vnode:get_cache_name(Partition, snapshot_cache),
    PVC_VLog = materializer_vnode:get_cache_name(Partition, pvc_snapshot_cache),
    PVC_Index = materializer_vnode:get_cache_name(Partition, pvc_index_cache),

    MatState = #mat_state{is_ready=false,
                          partition=Partition,
                          ops_cache=OpsCache,
                          snapshot_cache=SnapshotCache,
                          pvc_vlog_cache = PVC_VLog,
                          pvc_index_set = PVC_Index},

    %% ClockSI Cache
    PreparedCache = clocksi_vnode:get_cache_name(Partition, prepared),

    %% PVC State Cache
    PVCAtomicCache = clocksi_vnode:get_cache_name(Partition, pvc_state_table),

    Self = generate_server_name(Addr, Partition, Id),
    {ok, #state{id=Id,
                self=Self,
                partition=Partition,
                mat_state = MatState,
                pvc_atomic_cache = PVCAtomicCache,
                prepared_cache=PreparedCache}}.

handle_call({perform_read, Key, Type, Transaction}, Coordinator, SD0) ->
    ok = perform_read_internal(Coordinator, Key, Type, Transaction, [], SD0),
    {noreply, SD0};

handle_call(go_down, _Sender, SD0) ->
    {stop, shutdown, ok, SD0}.

handle_cast({perform_read_cast, Coordinator, Key, Type, Transaction}, SD0) ->
    ok = perform_read_internal(Coordinator, Key, Type, Transaction, [], SD0),
    {noreply, SD0};

handle_cast({pvc_fresh_read, Coordinator, IndexNode, Key, HasRead, VCaggr}, State) ->
    ok = pvc_fresh_read_internal(Coordinator,
                                 IndexNode,
                                 {#{mrvc_retries => 0}, Key},
                                 HasRead,
                                 VCaggr,
                                 State),
    {noreply, State};

handle_cast({pvc_vlog_read, Coordinator, Key, VCaggr}, State) ->
    #state{partition = SelfPartition, mat_state = MatState} = State,
    ok = pvc_vlog_read_internal(Coordinator, SelfPartition, {#{}, Key}, VCaggr, MatState),
    {noreply, State}.

%% @doc Given a key and a version vector clock, get the appropiate snapshot
%%
%%      It will scan the materializer for the specific snapshot, and reply
%%      to the coordinator the value of that snapshot, along with the commit
%%      vector clock time of that snapshot.
%%
-spec pvc_vlog_read_internal(term(), partition_id(), key(), pvc_vc(), #mat_state{}) -> ok.
pvc_vlog_read_internal(Coordinator, Partition, {InfoMap, Key}, MaxVC, MatState) ->
    {Took, Res} = timer:tc(materializer_vnode, pvc_read, [Key, MaxVC, MatState]),
    %% case materializer_vnode:pvc_read(Key, MaxVC, MatState) of
    case Res of
        {error, Reason} ->
            gen_fsm:send_event(Coordinator, {error, Reason});
        {ok, Value, CommitVC} ->
            gen_fsm:send_event(Coordinator, {pvc_readreturn, Partition, {InfoMap#{mat_read => Took}, Key}, Value, CommitVC, MaxVC})
    end.

%% @doc Wait until this PVC partition is ready to perform a read.
%%
%%      If this partition has not been read before, wait until the most
%%      recent vc of this partition is greater or equal that the passed
%%      VCaggr from the current transaction.
%%
%%      Once this happens, perform the read at this partition.
%%
-spec pvc_fresh_read_internal(term(), index_node(), key(), sets:set(), pvc_vc(), #state{}) -> ok.
pvc_fresh_read_internal(Coordinator, {Partition, _}=IndexNode, {InfoMap, Key}, HasRead, VCaggr, State = #state{
    pvc_atomic_cache = AtomicCache
}) ->
    {Took, MostRecentVC} = timer:tc(clocksi_vnode, pvc_get_most_recent_vc, [IndexNode, AtomicCache]),
    %% MostRecentVC = clocksi_vnode:pvc_get_most_recent_vc(IndexNode, AtomicCache),
    case pvc_check_time(Partition, MostRecentVC, VCaggr) of
        {not_ready, WaitTime} ->
            InfoMap1 = maps:update_with(mrvc_retries, fun(V) -> V + 1 end, InfoMap),
            erlang:send_after(WaitTime, self(), {pvc_wait_scan, Coordinator, IndexNode, {InfoMap1, Key}, HasRead, VCaggr}),
            ok;
        ready ->
            pvc_scan_and_read(Coordinator, IndexNode, {InfoMap#{get_mrvc => Took}, Key}, HasRead, VCaggr, State)
    end.

%% @doc Check if this partition is ready to proceed with a PVC read.
%%
%%      If it is not, will sleep for 1000 ms and try again.
%%
-spec pvc_check_time(partition_id(), pvc_vc(), pvc_vc()) -> ready
                                                          | {not_ready, non_neg_integer()}.

pvc_check_time(Partition, MostRecentVC, VCaggr) ->
    MostRecentTime = pvc_vclock:get_time(Partition, MostRecentVC),
    AggregateTime = pvc_vclock:get_time(Partition, VCaggr),
    case MostRecentTime < AggregateTime of
        true -> {not_ready, ?PVC_WAIT_MS};
        false -> ready
    end.

%% @doc Scan the replication log for a valid vector clock time for a read at this partition
%%
%%      The valid time is the maximum vector clock such that for every partition
%%      that the current partition has read, that partition time is smaller or
%%      equal than the VCaggr of the current transaction.
%%
-spec pvc_scan_and_read(
    Coordinator :: term(),
    index_node(),
    key(),
    sets:set(),
    pvc_vc(),
    #state{}
) -> ok.

pvc_scan_and_read(Coordinator, IndexNode, {InfoMap, Key}, HasRead, VCaggr, #state{
    pvc_atomic_cache = AtomicCache,
    partition = SelfPartition,
    mat_state = MatState
}) ->
    {Took, MaxVCRes} = timer:tc(?MODULE, pvc_find_maxvc, [IndexNode, HasRead, VCaggr, AtomicCache]),
    %% MaxVCRes = pvc_find_maxvc(IndexNode, HasRead, VCaggr, AtomicCache),
    case MaxVCRes of
        {error, Reason} ->
            gen_fsm:send_event(Coordinator, {error, Reason});

        {ok, MaxVC} ->
            pvc_vlog_read_internal(Coordinator, SelfPartition, {InfoMap#{find_maxvc => Took}, Key}, MaxVC, MatState)
    end.

%% @doc Scan the log for the maximum aggregate time that will be used for a read
-spec pvc_find_maxvc(index_node(), sets:set(), pvc_vc(), atom()) -> {ok, pvc_vc()}
                                                                       | {error, reason()}.

pvc_find_maxvc({CurrentPartition, _} = IndexNode, HasRead, VCaggr, AtomicCache) ->
    %% If this is the first partition we're reading, our MaxVC will be
    %% the current MostRecentVC at this partition
    MaxVC = case sets:size(HasRead) of
        0 -> clocksi_vnode:pvc_get_most_recent_vc(IndexNode, AtomicCache);
        _ -> logging_vnode:pvc_get_max_vc(IndexNode, sets:to_list(HasRead), VCaggr)
    end,

    %% If the selected time is too old, we should abort the read
    MaxSelectedTime = pvc_vclock:get_time(CurrentPartition, MaxVC),
    CurrentThresholdTime = pvc_vclock:get_time(CurrentPartition, VCaggr),
    ValidVersionTime = MaxSelectedTime >= CurrentThresholdTime,
    case ValidVersionTime of
        true ->
            {ok, MaxVC};

        false ->
            {error, maxvc_bad_vc}
    end.

perform_read_internal(Coordinator, Key, Type, Tx, [], State = #state{
    partition=Partition,
    prepared_cache=PreparedCache
}) ->
    %% TODO: Add support for read properties
    PropertyList = [],
    TxId = Tx#transaction.txn_id,
    TxLocalStartTime = TxId#tx_id.local_start_time,
    case check_clock(Key, TxLocalStartTime, PreparedCache, Partition) of
        {not_ready, Time} ->
            erlang:send_after(Time, self(), {perform_read_cast, Coordinator, Key, Type, Tx}),
            ok;
        ready ->
            return(Coordinator, Key, Type, Tx, PropertyList, State)
    end.

%% @doc check_clock: Compares its local clock with the tx timestamp.
%%      if local clock is behind, it sleeps the fms until the clock
%%      catches up. CLOCK-SI: clock skew.
%%
-spec check_clock(key(), clock_time(), ets:tid(), partition_id()) ->
                         {not_ready, clock_time()} | ready.
check_clock(Key, TxLocalStartTime, PreparedCache, Partition) ->
    Time = dc_utilities:now_microsec(),
    case TxLocalStartTime > Time of
        true ->
            {not_ready, (TxLocalStartTime - Time) div 1000 + 1};
        false ->
            check_prepared(Key, TxLocalStartTime, PreparedCache, Partition)
    end.

%% @doc check_prepared: Check if there are any transactions
%%      being prepared on the tranaction being read, and
%%      if they could violate the correctness of the read
-spec check_prepared(key(), clock_time(), ets:tid(), partition_id()) ->
                            ready | {not_ready, ?SPIN_WAIT}.
check_prepared(Key, TxLocalStartTime, PreparedCache, Partition) ->
    {ok, ActiveTxs} = clocksi_vnode:get_active_txns_key(Key, Partition, PreparedCache),
    check_prepared_list(Key, TxLocalStartTime, ActiveTxs).

-spec check_prepared_list(key(), clock_time(), [{txid(), clock_time()}]) ->
                                 ready | {not_ready, ?SPIN_WAIT}.
check_prepared_list(_Key, _TxLocalStartTime, []) ->
    ready;
check_prepared_list(Key, TxLocalStartTime, [{_TxId, Time}|Rest]) ->
    case Time =< TxLocalStartTime of
        true ->
            {not_ready, ?SPIN_WAIT};
        false ->
            check_prepared_list(Key, TxLocalStartTime, Rest)
    end.

%% @doc return:
%%  - Reads and returns the log of specified Key using replication layer.
-spec return({fsm, pid()} | pid(), key(), type(), #transaction{}, [], #state{}) -> ok.
return(Coordinator, Key, Type, Transaction, PropertyList,
       #state{mat_state=MatState}) ->
    %% TODO: Add support for read properties
    PropertyList = [],
    VecSnapshotTime = Transaction#transaction.vec_snapshot_time,
    case materializer_vnode:read(Key, Type, VecSnapshotTime, Transaction, MatState) of
        {ok, Snapshot} ->
            ReadReturn = {ok, {Key, Type, Snapshot}},
            Fallback = {ok, Snapshot},
            reply_to_coordinator(Coordinator, ReadReturn, Fallback);

        {error, Reason} ->
            reply_to_coordinator(Coordinator, {error, Reason})
    end,
    ok.

handle_info({perform_read_cast, Coordinator, Key, Type, Transaction}, SD0) ->
    ok = perform_read_internal(Coordinator, Key, Type, Transaction, [], SD0),
    {noreply, SD0};

handle_info({pvc_wait_scan, Coordinator, IndexNode, Key, HasRead, VCaggr}, State) ->
    ok = pvc_fresh_read_internal(Coordinator, IndexNode, Key, HasRead, VCaggr, State),
    {noreply, State};

handle_info(_Info, StateData) ->
    {noreply, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, _SD) ->
    ok.

%% @doc Send a message back to the transaction coordinator.
-spec reply_to_coordinator({fsm, pid()} | pid(), any()) -> ok.
reply_to_coordinator(Coordinator, Msg) ->
    reply_to_coordinator(Coordinator, Msg, Msg).

%% @doc Send a message back to the transaction coordinator.
%%
%%      Allows to specify a simple message if the coordinator is a simple
%%      server instead of the full-fsm coordinator.
%%
-spec reply_to_coordinator({fsm, pid()} | pid(), any(), any()) -> ok.
reply_to_coordinator({fsm, Sender}, Msg, _FallbackMsg) ->
    gen_fsm:send_event(Sender, Msg);

reply_to_coordinator(Coordinator, _Msg, FallbackMsg) ->
    gen_server:reply(Coordinator, FallbackMsg),
    ok.
