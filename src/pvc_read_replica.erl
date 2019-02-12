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

%% supervision tree
-export([start_link/2]).

%% external API
-export([start_replicas/2,
         stop_replicas/2,
         replica_ready/2]).

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

    %% Read replica of the last version committed by a key
    %% This is useful to compute VLog.last(key) without
    %% traversing the entire VLog
    committed_cache_replica :: atom()
}).

%% API

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

%% gen_server callbacks

init([Partition, Id]) ->
    VLog = materializer_vnode:get_cache_name(Partition, pvc_snapshot_cache),
    StateReplica = clocksi_vnode:get_cache_name(Partition, pvc_state_table),

    %% TODO(borja): Change name of ETS table
    Committed = clocksi_vnode:get_cache_name(Partition, committed_tx),

    Self = generate_replica_name(Partition, Id),

    {ok, #state{self=Self,
                partition = Partition,
                vlog_replica = VLog,
                committed_cache_replica = Committed,
                partition_state_replica = StateReplica}}.

handle_call(ready, _From, State) ->
    {reply, ready, State};

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(_Request, _From, _State) ->
    erlang:error(not_implemented).

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

%% Internal

-spec generate_replica_name(partition_id(), non_neg_integer()) -> atom().
generate_replica_name(Partition, Id) ->
    BinId = integer_to_binary(Id),
    BinPart = integer_to_binary(Partition),
    binary_to_atom(<<BinPart/binary, "_", BinId/binary>>, latin1).

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

%% Unused

handle_info(Info, State) ->
    lager:info("Unhandled msg ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
