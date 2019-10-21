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

-module(pvc_read_replica).

-behavior(gen_server).

-include("antidote.hrl").
-include("debug_log.hrl").

%% supervision tree
-export([start_link/2]).

%% replica management API
-export([start_replicas/2,
         stop_replicas/2,
         refresh_default/2,
         replica_ready/2]).

%% protocol API
-export([async_read/3, async_read/5]).

%% gen_fsm callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ignore_xref([start_link/2]).

-record(state, {
    %% Name of this read replica
    self :: atom(),
    %% Partition that this server is replicating
    partition :: partition_id(),

    %% Read replica of the VLog ETS table
    vlog_replica :: atom(),

    %% Default value and clock for empty keys
    default_bottom_value = <<>> :: any(),
    default_bottom_clock = pvc_vclock:new() :: pvc_vc()
}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Replica management API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Start a replica responsible for serving reads to this partion
%%
%%      To allow concurrency, multiple replicas are started. The `Id`
%%      parameter helps to distinguish them.
%%
%%      Since they replicate ETS tables stored in vnodes, they have
%%      to be started in the same physical node.
%%
%%
%%      This function is called from the supervisor dynamically
%%      (see pvc_read_replica_sup:start_replica/2)
%%
-spec start_link(Partition :: partition_id(),
                 Id :: non_neg_integer()) -> {ok, pid()} | ignore | {error, term()}.

start_link(Partition, Id) ->
    Name = {local, generate_replica_name(Partition, Id)},
    gen_server:start_link(Name, ?MODULE, [Partition, Id], []).

%% @doc Start `Count` read replicas for the given partition
-spec start_replicas(partition_id(), non_neg_integer()) -> ok.
start_replicas(Partition, Count) ->
    start_replicas_internal(Partition, Count).

%% @doc Stop `Count` read replicas for the given partition
-spec stop_replicas(partition_id(), non_neg_integer()) -> ok.
stop_replicas(Partition, Count) ->
    stop_replicas_internal(Partition, Count).

%% @doc Refresh `Count` read replicas with a new default value
-spec refresh_default(partition_id(), non_neg_integer()) -> ok.
refresh_default(Partition, Count) ->
    refresh_default_internal(Partition, Count).

%% @doc Check if all the read replicas at this node and partitions are ready
-spec replica_ready(partition_id(), non_neg_integer()) -> boolean().
replica_ready(_Partition, 0) ->
    true;

replica_ready(Partition, N) ->
    case gen_server:call(generate_replica_name(Partition, N), ready) of
        ready ->
            replica_ready(Partition, N - 1);
        _ ->
            false
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Protocol API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec async_read(coord_req_promise:promise(), partition_id(), key()) -> ok.
async_read(Promise, Partition, Key) ->
    Target = random_replica(Partition),
    gen_server:cast(Target, {read_rc, Promise, Key}).

-spec async_read(coord_req_promise:promise(), partition_id(), key(), ordsets:ordset(), pvc_vc()) -> ok.
async_read(Promise, Partition, Key, HasRead, VCaggr) ->
    Target = random_replica(Partition),
    case ordsets:is_element(Partition, HasRead) of
        true ->
            gen_server:cast(Target, {read_vlog, Promise, Key, VCaggr});
        false ->
            gen_server:cast(Target, {read_scan, Promise, Key, HasRead, VCaggr})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition, Id]) ->
    %% Materializer replica
    VLog = materializer_vnode:get_cache_name(Partition, pvc_snapshot_cache),
    {BottomValue, BottomClock} = materializer_vnode:pvc_get_default_value({Partition, node()}),

    Self = generate_replica_name(Partition, Id),
    {ok, #state{self = Self,
                partition = Partition,
                vlog_replica = VLog,
                default_bottom_value = BottomValue,
                default_bottom_clock = BottomClock}}.

handle_call(ready, _From, State) ->
    {reply, ready, State};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

%% @doc Refresh the default bottom value and clock
handle_call(refresh_default, _Sender, State=#state{partition=Partition}) ->
    {BottomValue, BottomClock} = materializer_vnode:pvc_get_default_value({Partition, node()}),
    {reply, ok, State#state{default_bottom_value=BottomValue, default_bottom_clock=BottomClock}};

handle_call(_Request, _From, _State) ->
    erlang:error(not_implemented).

handle_cast({read_rc, Promise, Key}, State=#state{partition=P, default_bottom_value=Bottom}) ->
    Value = antidote_pvc_vnode:rc_read(P, Key, Bottom),
    ok = coord_req_promise:resolve(Value, Promise),
    {noreply, State};

handle_cast({read_vlog, Promise, Key, VCaggr}, State = #state{vlog_replica=VLog,
                                                              default_bottom_value=DefaultValue,
                                                              default_bottom_clock=DefaultClock}) ->

    ok = read_vlog_internal(Promise, Key, VCaggr, VLog, {DefaultValue, DefaultClock}, [], []),
    {noreply, State};

handle_cast({read_scan, Promise, Key, HasRead, VCaggr}, State) ->
    ok = read_scan_internal(Promise, Key, HasRead, VCaggr, State),
    {noreply, State};

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

handle_info({wait_scan, Promise, Key, HasRead, VCaggr}, State) ->
    ok = read_scan_internal(Promise, Key, HasRead, VCaggr, State),
    {noreply, State};

handle_info(Info, State) ->
    lager:info("Unhandled msg ~p", [Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec read_scan_internal(coord_req_promise:promise(), key(), ordsets:ordset(), pvc_vc(), #state{}) -> ok.
read_scan_internal(Promise, Key, HasRead, VCaggr, State=#state{partition=Partition}) ->
    ?LAGER_LOG("most_recent_vc(~p)", [Partition]),

    MRVC = antidote_pvc_vnode:most_recent_vc(Partition),
    ?LAGER_LOG("MRVC = ~p", [MRVC]),
    case check_time(Partition, MRVC, VCaggr) of
        {not_ready, WaitTime} ->
            erlang:send_after(WaitTime, self(), {wait_scan, Promise, Key, HasRead, VCaggr}),
            ok;
        ready ->
            scan_and_read(Promise, Key, HasRead, VCaggr, State)
    end.

%% @doc Scan the replication log for a valid vector clock time for a read at this partition
%%
%%      The valid time is the maximum vector clock such that for every partition
%%      that the current partition has read, that partition time is smaller or
%%      equal than the VCaggr of the current transaction.
%%
-spec scan_and_read(coord_req_promise:promise(), key(), ordsets:ordset(), pvc_vc(), #state{}) -> ok.
scan_and_read(Promise, Key, HasRead, VCaggr, #state{partition=Partition,
                                                    vlog_replica=VLog,
                                                    default_bottom_value=BottomValue,
                                                    default_bottom_clock=ClockValue}) ->

    ?LAGER_LOG("scan_and_read(~p, ~p, ~p, ~p)", [Promise, Key, HasRead, VCaggr]),
    MaxVCRes = find_max_vc(Partition, HasRead, VCaggr),
    ?LAGER_LOG("MaxVC = ~p", [MaxVCRes]),
    case MaxVCRes of
        {error, Reason, FixedLog, FixedQueue} ->
            coord_req_promise:resolve({error, Reason, FixedLog, FixedQueue}, Promise);
        {ok, MaxVC, FixedLog, FixedQueue} ->
            read_vlog_internal(Promise, Key, MaxVC, VLog, {BottomValue, ClockValue}, FixedLog, FixedQueue)
    end.

%% @doc Scan the log for the maximum aggregate time that will be used for a read
-spec find_max_vc(Partition :: partition_id(),
                  HasRead :: ordsets:ordset(),
                  VCaggr :: pvc_vc()) -> {ok, pvc_vc()} | {error, reason()}.

find_max_vc(Partition, HasRead, VCaggr) ->
    {MaxVC, FixedLog, FixedQueue} = antidote_pvc_vnode:fix_partition_state_snapshot(Partition, HasRead, VCaggr),

    ?LAGER_LOG("Scanned MaxVC ~p", [MaxVC]),
    %% If the selected time is too old, we should abort the read
    MaxSelectedTime = pvc_vclock:get_time(Partition, MaxVC),
    CurrentThresholdTime = pvc_vclock:get_time(Partition, VCaggr),
    ValidVersionTime = MaxSelectedTime >= CurrentThresholdTime,
    case ValidVersionTime of
        true ->
            {ok, MaxVC, FixedLog, FixedQueue};

        false ->
            {error, maxvc_bad_vc, FixedLog, FixedQueue}
    end.

%% @doc Given a key and a version vector clock, get the appropiate snapshot
%%
%%      It will scan the materializer for the specific snapshot, and reply
%%      to the coordinator the value of that snapshot, along with the commit
%%      vector clock time of that snapshot.
%%
-spec read_vlog_internal(coord_req_promise:promise(), key(), pvc_vc(), atom(), tuple(), [_], [_]) -> ok.
read_vlog_internal(Promise, Key, MaxVC, VLogCache, DefaultBottom, FixedLog, FixedQueue) ->
    ?LAGER_LOG("vlog read(~p, ~p, ~p)", [Promise, Key, MaxVC]),
    {ok, Value, VersionVC} = materializer_vnode:pvc_read_replica(Key, MaxVC, VLogCache, DefaultBottom),
    coord_req_promise:resolve({ok, Value, VersionVC, MaxVC, FixedLog, FixedQueue}, Promise).

%% @doc Check if this partition is ready to proceed with a PVC read.
%%
%%      If it is not, will sleep for 1000 ms and try again.
%%
-spec check_time(partition_id(), pvc_vc(), pvc_vc()) -> ready | {not_ready, non_neg_integer()}.
check_time(Partition, MostRecentVC, VCaggr) ->
    MostRecentTime = pvc_vclock:get_time(Partition, MostRecentVC),
    AggregateTime = pvc_vclock:get_time(Partition, VCaggr),
    case MostRecentTime < AggregateTime of
        true ->
            ok = antidote_stats_collector:log_partition_not_ready(Partition),
            {not_ready, ?PVC_WAIT_MS};
        false ->
            ready
    end.

-spec generate_replica_name(partition_id(), non_neg_integer()) -> atom().
generate_replica_name(Partition, Id) ->
    BinId = integer_to_binary(Id),
    BinPart = integer_to_binary(Partition),
    binary_to_atom(<<BinPart/binary, "_", BinId/binary>>, latin1).

-spec random_replica(partition_id()) -> atom().
random_replica(Partition) ->
    generate_replica_name(Partition, rand:uniform(?READ_CONCURRENCY)).

-spec start_replicas_internal(partition_id(), non_neg_integer()) -> ok.
start_replicas_internal(_Partition, 0) ->
    ok;

start_replicas_internal(Partition, N) ->
    case pvc_read_replica_sup:start_replica(Partition, N) of
        {ok, _} ->
            start_replicas_internal(Partition, N - 1);
        {error, {already_started, _}} ->
            start_replicas_internal(Partition, N - 1);
        _Other ->
            lager:debug("Unable to start pvc read replica for ~p, will retry", [Partition]),
            try
                ok = gen_server:call(generate_replica_name(Partition, N), shutdown)
            catch _:_ ->
                ok
            end,
            start_replicas_internal(Partition, N - 1)
    end.

-spec stop_replicas_internal(partition_id(), non_neg_integer()) -> ok.
stop_replicas_internal(_Partition, 0) ->
    ok;

stop_replicas_internal(Partition, N) ->
    try
        ok = gen_server:call(generate_replica_name(Partition, N), shutdown)
    catch _:_ ->
        ok
    end,
    stop_replicas_internal(Partition, N - 1).

-spec refresh_default_internal(partition_id(), non_neg_integer()) -> ok.
refresh_default_internal(_Partition, 0) ->
    ok;

refresh_default_internal(Partition, N) ->
    Target = generate_replica_name(Partition, N),
    ok = gen_server:call(Target, refresh_default),
    refresh_default_internal(Partition, N - 1).

%% Unused

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
