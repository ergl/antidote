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
-export([async_read/5]).

%% gen_fsm callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    %% Name of this read replica
    self :: atom(),
    %% Partition that this server is replicating
    partition :: partition_id(),

    %% Read replica of
    %% MostRecentVC
    %% SeqNumber
    partition_state_replica :: atom(),

    %% Read replica of the VLog ETS table
    vlog_replica :: atom(),

    %% Default value and clock for empty keys
    default_bottom_value = <<>> :: any(),
    default_bottom_clock = pvc_vclock:new() :: pvc_vc(),

    %% Read replica of the last version committed by a key
    %% This is useful to compute VLog.last(key) without
    %% traversing the entire VLog
    committed_cache_replica :: atom()
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
-spec start_link(
    partition_id(),
    non_neg_integer()
) -> {ok, pid()} | ignore | {error, term()}.

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

-spec async_read(pid(), partition_id(), key(), ordsets:ordset(), pvc_vc()) -> ok.
async_read(ReplyTo, Partition, Key, HasRead, VCaggr) ->
    Target = random_replica(Partition),
    case ordsets:is_element(Partition, HasRead) of
        true ->
            gen_server:cast(Target, {read_vlog, ReplyTo, Key, VCaggr});
        false ->
            gen_server:cast(Target, {read_scan, ReplyTo, Key, HasRead, VCaggr})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition, Id]) ->
    %% Materializer replica
    VLog = materializer_vnode:get_cache_name(Partition, pvc_snapshot_cache),
    {BottomValue, BottomClock} = materializer_vnode:pvc_get_default_value({Partition, node()}),

    %% Partition replica
    StateReplica = clocksi_vnode:get_cache_name(Partition, pvc_state_table),
    %% TODO(borja/pvc-ccoord): Change name of ETS table
    Committed = clocksi_vnode:get_cache_name(Partition, committed_tx),

    Self = generate_replica_name(Partition, Id),

    {ok, #state{self = Self,
                partition = Partition,
                vlog_replica = VLog,
                default_bottom_value = BottomValue,
                default_bottom_clock = BottomClock,
                committed_cache_replica = Committed,
                partition_state_replica = StateReplica}}.

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

handle_cast({read_vlog, ReplyTo, Key, VCaggr}, State = #state{vlog_replica=VLog,
                                                              default_bottom_value=DefaultValue,
                                                              default_bottom_clock=DefaultClock}) ->

    ok = read_vlog_internal(ReplyTo, Key, VCaggr, VLog, {DefaultValue, DefaultClock}),
    {noreply, State};

handle_cast({read_scan, ReplyTo, Key, HasRead, VCaggr}, State) ->
    ok = read_scan_internal(ReplyTo, Key, HasRead, VCaggr, State),
    {noreply, State};

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

handle_info({wait_scan, ReplyTo, Key, HasRead, VCaggr}, State) ->
    ok = read_scan_internal(ReplyTo, Key, HasRead, VCaggr, State),
    {noreply, State};

handle_info(Info, State) ->
    lager:info("Unhandled msg ~p", [Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec read_scan_internal(pid(), key(), ordsets:ordset(), pvc_vc(), #state{}) -> ok.
read_scan_internal(ReplyTo, Key, HasRead, VCaggr, State) ->
    #state{partition=Partition,
           partition_state_replica=ReplicaState} = State,

    %% FIXME(borja): Remove hack {Partition, node()}
    %% Table should be at this node
    ?LAGER_LOG("get_mrvc({~p,~p})", [Partition, node()]),

    MRVC = clocksi_vnode:pvc_get_most_recent_vc({Partition, node()}, ReplicaState),
    ?LAGER_LOG("MRVC = ~p", [MRVC]),
    case check_time(Partition, MRVC, VCaggr) of
        {not_ready, WaitTime} ->
            ?LAGER_LOG("Partition not ready, will wait ~p", [WaitTime]),
            erlang:send_after(WaitTime, self(), {wait_scan, ReplyTo, Key, HasRead, VCaggr}),
            ok;
        ready ->
            scan_and_read(ReplyTo, Key, HasRead, VCaggr, State)
    end.

%% @doc Scan the replication log for a valid vector clock time for a read at this partition
%%
%%      The valid time is the maximum vector clock such that for every partition
%%      that the current partition has read, that partition time is smaller or
%%      equal than the VCaggr of the current transaction.
%%
-spec scan_and_read(pid(), key(), ordsets:ordset(), pvc_vc(), #state{}) -> ok.
scan_and_read(ReplyTo, Key, HasRead, VCaggr, #state{partition=Partition,
                                                    vlog_replica=VLog,
                                                    default_bottom_value=BottomValue,
                                                    default_bottom_clock=ClockValue,
                                                    partition_state_replica=ReplicaState}) ->

    ?LAGER_LOG("scan_and_read(~p, ~p, ~p, ~p)", [ReplyTo, Key, HasRead, VCaggr]),
    MaxVCRes = find_max_vc(Partition, HasRead, VCaggr, ReplicaState),
    ?LAGER_LOG("MaxVC = ~p", [MaxVCRes]),
    case MaxVCRes of
        {error, Reason} ->
            reply(ReplyTo, {error, Reason});
        {ok, MaxVC} ->
            read_vlog_internal(ReplyTo, Key, MaxVC, VLog, {BottomValue, ClockValue})
    end.

%% @doc Scan the log for the maximum aggregate time that will be used for a read
-spec find_max_vc(
    partition_id(),
    ordsets:ordset(),
    pvc_vc(),
    atom()
) -> {ok, pvc_vc()} | {error, reason()}.

find_max_vc(Partition, HasRead, VCaggr, ReplicaState) ->
    %% If this is the first partition we're reading, our MaxVC will be
    %% the current MostRecentVC at this partition
    MaxVC = case ordsets:size(HasRead) of
        0 ->
            %% FIXME(borja): Remove hack {Partition, node()}
            %% Table should be at this node
            ?LAGER_LOG("get_mrvc({~p,~p})", [Partition, node()]),
            clocksi_vnode:pvc_get_most_recent_vc({Partition, node()}, ReplicaState);
        _ ->
            ?LAGER_LOG("logging_vnode:pvc_get_max_vc({~p,~p})", [Partition, node()]),
            logging_vnode:pvc_get_max_vc({Partition, node()}, ordsets:to_list(HasRead), VCaggr)
    end,

    ?LAGER_LOG("Scanned MaxVC ~p", [MaxVC]),
    %% If the selected time is too old, we should abort the read
    MaxSelectedTime = pvc_vclock:get_time(Partition, MaxVC),
    CurrentThresholdTime = pvc_vclock:get_time(Partition, VCaggr),
    ValidVersionTime = MaxSelectedTime >= CurrentThresholdTime,
    case ValidVersionTime of
        true ->
            {ok, MaxVC};

        false ->
            {error, maxvc_bad_vc}
    end.

%% @doc Given a key and a version vector clock, get the appropiate snapshot
%%
%%      It will scan the materializer for the specific snapshot, and reply
%%      to the coordinator the value of that snapshot, along with the commit
%%      vector clock time of that snapshot.
%%
-spec read_vlog_internal(pid(), key(), pvc_vc(), atom(), tuple()) -> ok.
read_vlog_internal(ReplyTo, Key, MaxVC, VLogCache, DefaultBottom) ->
    ?LAGER_LOG("vlog read(~p, ~p, ~p)", [ReplyTo, Key, MaxVC]),
    case materializer_vnode:pvc_read_replica(Key, MaxVC, VLogCache, DefaultBottom) of
        {error, Reason} ->
            reply(ReplyTo, {error, Reason});
        {ok, Value, VersionVC} ->
            reply(ReplyTo, {ok, Value, VersionVC, MaxVC})
    end.

-spec reply(pid(), term()) -> ok.
reply(To, Msg) when is_pid(To) ->
    To ! Msg,
    ok.

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
