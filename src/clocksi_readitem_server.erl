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
         check_servers_ready/0,
         handle_info/2,
         handle_sync_event/4,
         terminate/2]).

%% States
-export([read_data_item/4,
         async_read_data_item/5,
         check_partition_ready/3,
         start_read_servers/2,
         stop_read_servers/2]).

%% Spawn
-record(state, {
    self :: atom(),
    id :: non_neg_integer(),
    mat_state :: #mat_state{},
    partition :: partition_id(),
    prepared_cache :: cache_id()
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

-spec start_read_servers(partition_id(), non_neg_integer()) -> 0.
start_read_servers(Partition, Count) ->
    Addr = node(),
    start_read_servers_internal(Addr, Partition, Count).

-spec stop_read_servers(partition_id(), non_neg_integer()) -> ok.
stop_read_servers(Partition, Count) ->
    Addr = node(),
    stop_read_servers_internal(Addr, Partition, Count).

-spec read_data_item(index_node(), key(), type(), tx()) -> {error, term()} | {ok, snapshot()}.
read_data_item({Partition, Node}, Key, Type, Transaction) ->
    try
        gen_server:call({global, generate_random_server_name(Node, Partition)},
                        {perform_read, Key, Type, Transaction}, infinity)
    catch
        _:Reason ->
            lager:debug("Exception caught: ~p, starting read server to fix", [Reason]),
            check_server_ready([{Partition, Node}]),
            read_data_item({Partition, Node}, Key, Type, Transaction)
    end.

-spec async_read_data_item(index_node(), key(), type(), tx(), term()) -> ok.
async_read_data_item({Partition, Node}=IndexNode, Key, Type, Transaction, Coordinator) ->
    To = {global, generate_random_server_name(Node, Partition)},
    Msg = case Transaction#transaction.transactional_protocol of
        pvc ->
            {pvc_perform_read_cast, Coordinator, IndexNode, Key, Type, Transaction};
        _ ->
            {perform_read_cast, Coordinator, Key, Type, Transaction}
    end,
    gen_server:cast(To, Msg).

%% @doc This checks all partitions in the system to see if all read
%%      servers have been started up.
%%      Returns true if they have been, false otherwise.
-spec check_servers_ready() -> boolean().
check_servers_ready() ->
    PartitionList = dc_utilities:get_all_partitions_nodes(),
    check_server_ready(PartitionList).

-spec check_server_ready([index_node()]) -> boolean().
check_server_ready([]) ->
    true;
check_server_ready([{Partition, Node}|Rest]) ->
    try
        Result = riak_core_vnode_master:sync_command({Partition, Node},
                                                     {check_servers_ready},
                                                     ?CLOCKSI_MASTER,
                                                     infinity),
        case Result of
            false ->
                false;
            true ->
                check_server_ready(Rest)
        end
    catch
        _:_Reason ->
            false
    end.

-spec check_partition_ready(node(), partition_id(), non_neg_integer()) -> boolean().
check_partition_ready(_Node, _Partition, 0) ->
    true;
check_partition_ready(Node, Partition, Num) ->
    case global:whereis_name(generate_server_name(Node, Partition, Num)) of
        undefined ->
            false;
        _Res ->
            check_partition_ready(Node, Partition, Num-1)
    end.



%%%===================================================================
%%% Internal
%%%===================================================================

-spec start_read_servers_internal(node(), partition_id(), non_neg_integer()) -> non_neg_integer().
start_read_servers_internal(_Node, _Partition, 0) ->
    0;
start_read_servers_internal(Node, Partition, Num) ->
    case clocksi_readitem_sup:start_fsm(Partition, Num) of
        {ok, _Id} ->
            start_read_servers_internal(Node, Partition, Num-1);
        {error, {already_started, _}} ->
            start_read_servers_internal(Node, Partition, Num-1);
        Err ->
            lager:debug("Unable to start clocksi read server for ~w, will retry", [Err]),
            try
                gen_server:call({global, generate_server_name(Node, Partition, Num)}, {go_down})
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
    gen_server:call({global, generate_server_name(Node, Partition, Num)}, {go_down})
    catch
        _:_Reason->
           ok
    end,
    stop_read_servers_internal(Node, Partition, Num-1).

-spec generate_server_name(node(), partition_id(), non_neg_integer()) -> atom().
generate_server_name(Node, Partition, Id) ->
    list_to_atom(integer_to_list(Id) ++ integer_to_list(Partition) ++ atom_to_list(Node)).

-spec generate_random_server_name(node(), partition_id()) -> atom().
generate_random_server_name(Node, Partition) ->
    generate_server_name(Node, Partition, rand_compat:uniform(?READ_CONCURRENCY)).

init([Partition, Id]) ->
    Addr = node(),
    OpsCache = materializer_vnode:get_cache_name(Partition, ops_cache),
    SnapshotCache = materializer_vnode:get_cache_name(Partition, snapshot_cache),
    PreparedCache = clocksi_vnode:get_cache_name(Partition, prepared),
    MatState = #mat_state{ops_cache=OpsCache, snapshot_cache=SnapshotCache, partition=Partition, is_ready=false},
    Self = generate_server_name(Addr, Partition, Id),
    {ok, #state{partition=Partition, id=Id,
                mat_state = MatState,
                prepared_cache=PreparedCache, self=Self}}.

handle_call({perform_read, Key, Type, Transaction}, Coordinator, SD0) ->
    ok = perform_read_internal(Coordinator, Key, Type, Transaction, [], SD0),
    {noreply, SD0};

handle_call({go_down}, _Sender, SD0) ->
    {stop, shutdown, ok, SD0}.

handle_cast({perform_read_cast, Coordinator, Key, Type, Transaction}, SD0) ->
    ok = perform_read_internal(Coordinator, Key, Type, Transaction, [], SD0),
    {noreply, SD0};

handle_cast({pvc_perform_read_cast, Coordinator, IndexNode, Key, Type, Tx}, State) ->
    ok = pvc_perform_read_internal(Coordinator, IndexNode, Key, Type, Tx, State),
    {noreply, State}.

%% @doc Performs a PVC read in this partition.
%%
%%      During the first phase, it finds the maximum vector clock
%%      valid for this specific read. If this partition has been read
%%      before, use the included VCaggr in the current transaction.
%%
%%      If it has been not, it will find the vector clock in the CLog
%%      (represented as the replication log in Antidote).
%%
-spec pvc_perform_read_internal(
    {fsm, pid()} | pid(),
    index_node(),
    key(),
    type(),
    #transaction{},
    #state{}
) -> ok.

pvc_perform_read_internal(Coordinator, IndexNode, Key, Type, Tx, State = #state{
    partition = CurrentPartition
}) ->

%%    lager:info("{~p} PVC read ~p from ~p", [erlang:phash2(Tx#transaction.txn_id), Key, CurrentPartition]),

    %% Sanity check
    pvc = Tx#transaction.transactional_protocol,

    HasRead = Tx#transaction.pvc_meta#pvc_tx_meta.hasread,
    case sets:is_element(CurrentPartition, HasRead) of
        false ->
            pvc_wait_scan(IndexNode, Coordinator, Tx, Key, Type, State);

        true ->
            VCaggr = Tx#transaction.pvc_meta#pvc_tx_meta.time#pvc_time.vcaggr,
%%            lager:info("{~p} PVC ~p was read before, using ~p", [erlang:phash2(Tx#transaction.txn_id), CurrentPartition, dict:to_list(VCaggr)]),
            pvc_perform_read(Coordinator, Key, Type, VCaggr, State)
    end.

%% @doc Wait until this PVC partition is ready to perform a read.
%%
%%      If this partition has not been read before, wait until the most
%%      recent vc of this partition is greater or equal that the passed
%%      VCaggr from the current transaction.
%%
%%      Once this happens, perform the read at this partition.
%%
-spec pvc_wait_scan(
    index_node(),
    {fsm, pid()} | pid(),
    #transaction{},
    key(),
    type(),
    #state{}
) -> ok.

pvc_wait_scan(IndexNode, Coordinator, Transaction, Key, Type, State = #state{
    partition = CurrentPartition
}) ->

    VCaggr = Transaction#transaction.pvc_meta#pvc_tx_meta.time#pvc_time.vcaggr,
    {ok, MostRecentVC} = clocksi_vnode:pvc_get_most_recent_vc(IndexNode),
    case pvc_check_time(Transaction, CurrentPartition, MostRecentVC, VCaggr) of
        {not_ready, WaitTime} ->
            erlang:send_after(WaitTime, self(), {pvc_wait_scan, IndexNode, Coordinator, Transaction, Key, Type}),
            ok;

        ready ->
            pvc_scan_and_read(Coordinator, Key, Type, Transaction, State)
    end.

%% @doc Check if this partition is ready to proceed with a PVC read.
%%
%%      If it is not, will sleep for 1000 ms and try again.
%%
-spec pvc_check_time(
    #transaction{},
    partition_id(),
    vectorclock_partition:partition_vc(),
    vectorclock_partition:partition_vc()
) -> ready | {not_ready, non_neg_integer()}.

pvc_check_time(_, Partition, MostRecentVC, VCaggr) ->
    MostRecentTime = vectorclock_partition:get_partition_time(Partition, MostRecentVC),
    AggregateTime = vectorclock_partition:get_partition_time(Partition, VCaggr),
    case MostRecentTime < AggregateTime of
        true ->
%%            lager:info("{~p} PVC read MRVC check, NOT READY, VCaggr (~p) > MostRecentVC (~p)", [erlang:phash2(TxId), AggregateTime, MostRecentTime]),
            {not_ready, ?PVC_WAIT_MS};
        false ->
%%            lager:info("{~p} PVC read MRVC check, READY, VCaggr (~p) <= MostRecentVC (~p)", [erlang:phash2(TxId), AggregateTime, MostRecentTime]),
            ready
    end.

%% @doc Scan the replication log for a valid vector clock time for a read at this partition
%%
%%      The valid time is the maximum vector clock such that for every partition
%%      that the current partition has read, that partition time is smaller or
%%      equal than the VCaggr of the current transaction.
%%
-spec pvc_scan_and_read(
    {fsm, pid()} | pid(),
    key(),
    type(),
    #transaction{},
    #state{}
) -> ok.

pvc_scan_and_read(Coordinator, Key, Type, Transaction, State = #state{
    partition = Partition
}) ->
    %% Sanity check
    {Partition, _}=Node = log_utilities:get_key_partition(Key),
    MaxVCRes = pvc_find_maxvc(Node, Transaction),
    case MaxVCRes of
        {error, Reason} ->
            reply_to_coordinator(Coordinator, {error, Reason});

        {ok, MaxVC} ->
            %% lager:info("{~p} PVC read ~p found MaxVC ~p", [erlang:phash2(Transaction#transaction.txn_id), Key, dict:to_list(MaxVC)]),
            pvc_perform_read(Coordinator, Key, Type, MaxVC, State)
    end.

%% @doc Scan the log for the maximum aggregate time that will be used for a read
-spec pvc_find_maxvc(index_node(), #transaction{}) -> {ok, vectorclock_partition:partition_vc()}
                                                    | {error, reason()}.

pvc_find_maxvc({CurrentPartition, _} = IndexNode, #transaction{
    %% Sanity check
    transactional_protocol = pvc,
    pvc_meta = #pvc_tx_meta{
        hasread = HasRead,
        time = #pvc_time{
            vcaggr = VCaggr
        }
    }
}) ->

    %% Got to CLog anyway, verify correctness
    %% If this always holds, we can optimize here
    {ok, MaxVC} = logging_vnode:pvc_get_max_vc(IndexNode, sets:to_list(HasRead), VCaggr),
    {ok, MostRecentVC} = clocksi_vnode:pvc_get_most_recent_vc(IndexNode),

    %% If this is the first partition we read, we should get the current MRVC
    %% If sets:size(HasRead) == 0 then pick MostRecentVC instead
    true = vectorclock:le(MaxVC, MostRecentVC),
    lager:info("Invariant: ~p =< ~p", [dict:to_list(MaxVC), dict:to_list(MostRecentVC)]),

    %% If the selected time is too old, we should abort the read
    MaxSelectedTime = vectorclock_partition:get_partition_time(CurrentPartition, MaxVC),
    CurrentThresholdTime = vectorclock_partition:get_partition_time(CurrentPartition, VCaggr),
    ValidVersionTime = MaxSelectedTime >= CurrentThresholdTime,
    case ValidVersionTime of
        true ->
            {ok, MaxVC};

        false ->
            {error, abort}
    end.

%% @doc Given a key and a version vector clock, get the appropiate snapshot
%%
%%      It will scan the materializer for the specific snapshot, and reply
%%      to the coordinator the value of that snapshot, along with the commit
%%      vector clock time of that snapshot.
%%
-spec pvc_perform_read(
    {fsm, pid()} | pid(),
    key(),
    type(),
    vectorclock_partition:partition_vc(),
    #state{}
) -> ok.

pvc_perform_read(Coordinator, Key, Type, MaxVC, #state{mat_state=MatState}) ->
    case materializer_vnode:pvc_read(pvc, Key, Type, MaxVC, MatState) of
        {error, Reason} ->
            reply_to_coordinator(Coordinator, {error, Reason});

        {ok, Snapshot, CommitVC} ->
            Value = Type:value(Snapshot),
            CoordReturn = {pvc_readreturn, {Key, Value, CommitVC, MaxVC}},

            %% TODO(borja): Check when is this triggered
            ServerReturn = {ok, Value},
            reply_to_coordinator(Coordinator, CoordReturn, ServerReturn)
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

handle_info({pvc_wait_scan, IndexNode, Coordinator, Transaction, Key, Type}, State) ->
    ok = pvc_wait_scan(IndexNode, Coordinator, Transaction, Key, Type, State),
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
