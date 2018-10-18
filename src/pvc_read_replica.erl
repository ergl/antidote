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

%% For partition_id, index_node, key, val
-include("antidote.hrl").
-include("pvc.hrl").

-define(NUM_REPLICAS, 20).

%% supervision tree
-export([start_link/2]).

%% external API
-export([start_replicas/2,
         stop_replicas/2,
         all_ready/0,
         replica_ready/3]).

%% protocol API
-export([async_read/3]).

%% gen_fsm callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    %% Name of this read replica
    self :: binary(),
    %% Partition that this server is replicating
    partition :: partition_id(),

    %% Read replica of
    %% MostRecentVC
    %% SeqNumber
    partition_state_replica :: atom(),

    %% Read replica of the VLog ETS table
    vlog_replica :: atom()
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
-spec start_link(partition_id(), non_neg_integer()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Partition, Id) ->
    Name = {global, generate_replica_name(node(), Partition, Id)},
    gen_server:start_link(Name, ?MODULE, [Partition, Id], []).

%% @doc Start `Count` read replicas for the given partition
-spec start_replicas(partition_id(), non_neg_integer()) -> ok.
start_replicas(Partition, Count) ->
    start_replicas(node(), Partition, Count).

%% @doc Stop `Count` read replicas for the given partition
-spec stop_replicas(partition_id(), non_neg_integer()) -> ok.
stop_replicas(Partition, Count) ->
    stop_replicas(node(), Partition, Count).

%% @doc Check if all read replicas everywhere are ready
-spec all_ready() -> boolean().
all_ready() ->
    all_ready(dc_utilities:get_all_partitions_nodes()).

%% @doc Check if all the read replicas at this node and partitions are ready
-spec replica_ready(node(), partition_id(), non_neg_integer()) -> boolean().
replica_ready(_Node, _Partition, 0) ->
    true;
replica_ready(Node, Partition, N) ->
    case global:whereis_name(generate_replica_name(Node, Partition, N)) of
        undefined ->
            false;
        _ ->
            replica_ready(Node, Partition, N - 1)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Protocol API
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Perform an asynchronous read in a replica
-spec async_read(key(), sets:set(), pvc_vc()) -> ok.
async_read(Key, HasRead, VCaggr) ->
    %% If not read, go through wait process
    {Partition, Node}=IndexNode = log_utilities:get_key_partition(Key),
    To = {global, random_replica(Node, Partition)},
    Msg = case sets:is_element(Partition, HasRead) of
              true -> {read, self(), Key, VCaggr};
              false -> {read_with_scan, self(), IndexNode, Key, HasRead, VCaggr}
          end,
    gen_server:cast(To, Msg).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% gen_server callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Partition, Id]) ->
    VLog = pvc_storage_vnode:get_cache_name(Partition, pvc_storage),
    StateReplica = pvc_storage_vnode:get_cache_name(Partition, pvc_partition_state),

    Self = generate_replica_name(node(), Partition, Id),

    {ok, #state{self=Self,
                partition = Partition,
                vlog_replica = VLog,
                partition_state_replica = StateReplica}}.

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(_Request, _From, _State) ->
    erlang:error(not_implemented).

handle_cast({read, Coordinator, Key, VCaggr}, State) ->
    #state{partition = SelfPartition, vlog_replica = VLog} = State,
    ok = vlog_read_internal(Coordinator, SelfPartition, Key, VCaggr, VLog),
    {noreply, State};

handle_cast({read_with_scan, Coordinator, IndexNode, Key, HasRead, VCaggr}, State) ->
    ok = read_with_scan_internal(Coordinator, IndexNode, Key, HasRead, VCaggr, State),
    {noreply, State};

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

handle_info({wait_scan, Coordinator, IndexNode, Key, HasRead, VCaggr}, State) ->
    ok = read_with_scan_internal(Coordinator, IndexNode, Key, HasRead, VCaggr, State),
    {noreply, State};

handle_info(Info, State) ->
    lager:info("Unhandled msg ~p", [Info]),
    {noreply, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @doc Given a key and a version vector clock, get the appropiate snapshot
%%
%%      It will scan the materializer for the specific snapshot, and reply
%%      to the coordinator the value of that snapshot, along with the commit
%%      vector clock time of that snapshot.
%%
-spec vlog_read_internal(term(), partition_id(), key(), pvc_vc(), atom()) -> ok.
vlog_read_internal(Coordinator, Partition, Key, MaxVC, VLog) ->
    {Value, CommitVC} = pvc_storage_vnode:read_key(Partition, Key, MaxVC, VLog),
    gen_fsm:send_event(Coordinator, {readreturn, Partition, Key, Value, CommitVC, MaxVC}).

-spec read_with_scan_internal(term(), index_node(), key(), sets:set(), pvc_vc(), #state{}) -> ok.
read_with_scan_internal(Coordinator, {Partition, _}=IndexNode, Key, HasRead, VCaggr, State = #state{
    partition_state_replica = AtomicCache
}) ->
    MostRecentVC = pvc_storage_vnode:get_most_recent_vc(IndexNode, AtomicCache),
    case check_time(Partition, MostRecentVC, VCaggr) of
        {not_ready, WaitTime} ->
            erlang:send_after(WaitTime, self(), {wait_scan, Coordinator, IndexNode, Key, HasRead, VCaggr}),
            ok;
        ready ->
            scan_and_read(Coordinator, IndexNode, Key, HasRead, VCaggr, State)
    end.

%% @doc Check if this partition is ready to proceed with a PVC read.
%%
%%      If it is not, will sleep for 1000 ms and try again.
%%
-spec check_time(partition_id(), pvc_vc(), pvc_vc()) -> ready
                                                     |  {not_ready, non_neg_integer()}.

check_time(Partition, MostRecentVC, VCaggr) ->
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
-spec scan_and_read(term(), index_node(), key(), sets:set(), pvc_vc(), #state{}) -> ok.
scan_and_read(Coordinator, IndexNode, Key, HasRead, VCaggr, #state{
    partition_state_replica = AtomicCache,
    partition = SelfPartition,
    vlog_replica = VLog
}) ->
    MaxVCRes = find_max_vc(IndexNode, HasRead, VCaggr, AtomicCache),
    case MaxVCRes of
        {error, Reason} ->
            gen_fsm:send_event(Coordinator, {error, Reason});

        {ok, MaxVC} ->
            vlog_read_internal(Coordinator, SelfPartition, Key, MaxVC, VLog)
    end.

%% @doc Scan the log for the maximum aggregate time that will be used for a read
-spec find_max_vc(index_node(), sets:set(), pvc_vc(), atom()) -> {ok, pvc_vc()}
                                                               | {error, reason()}.
find_max_vc({CurrentPartition, _} = IndexNode, HasRead, VCaggr, AtomicCache) ->
    %% If this is the first partition we're reading, our MaxVC will be
    %% the current MostRecentVC at this partition
    MaxVC = case sets:size(HasRead) of
        0 -> pvc_storage_vnode:get_most_recent_vc(IndexNode, AtomicCache);
        _ -> pvc_storage_vnode:get_max_vc(IndexNode, sets:to_list(HasRead), VCaggr)
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

-spec generate_replica_name(node(), partition_id(), non_neg_integer()) -> binary().
generate_replica_name(Node, Partition, Id) ->
    BinId = integer_to_binary(Id),
    BinPart = integer_to_binary(Partition),
    BinNode = atom_to_binary(Node, latin1),
    <<BinId/binary, "=", BinPart/binary, "=", BinNode/binary>>.

-spec random_replica(node(), partition_id()) -> binary().
random_replica(Node, Partition) ->
    generate_replica_name(Node, Partition, rand_compat:uniform(?NUM_REPLICAS)).

-spec start_replicas(node(), partition_id(), non_neg_integer()) -> ok.
start_replicas(_Node, _Partition, 0) ->
    ok;

start_replicas(Node, Partition, N) ->
    case pvc_read_replica_sup:start_replica(Partition, N) of
        {ok, _} ->
            start_replicas(Node, Partition, N - 1);
        {error, {already_started, _}} ->
            start_replicas(Node, Partition, N - 1);
        _Other ->
            lager:debug("Unable to start pvc read replica for ~p, will retry", [Partition]),
            try
                ok = gen_server:call({global, generate_replica_name(Node, Partition, N)}, shutdown)
            catch _:_ ->
                ok
            end,
            start_replicas(Node, Partition, N - 1)
    end.

-spec stop_replicas(node(), partition_id(), non_neg_integer()) -> ok.
stop_replicas(_Node, _Partition, 0) ->
    ok;

stop_replicas(Node, Partition, N) ->
    try
        ok = gen_server:call({global, generate_replica_name(Node, Partition, N)}, shutdown)
    catch _:_ ->
        ok
    end,
    stop_replicas(Node, Partition, N - 1).

-spec all_ready([index_node()]) -> boolean().
all_ready([]) ->
    true;

all_ready([Node | Rest]) ->
    Res = try
        riak_core_vnode_master:sync_command(Node,
                                            tables_ready,
                                            pvc_storage_vnode_master,
                                            infinity)
    catch _:_  ->
        %% If virtual node is not up and runnning for any reason, return false
        false
    end,
    case Res of
        false ->
            false;
        true ->
            all_ready(Rest)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Unused
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
