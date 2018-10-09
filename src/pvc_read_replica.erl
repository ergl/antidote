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
         all_ready/0,
         replica_ready/3]).

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

%% gen_server callbacks

init([Partition, Id]) ->
    VLog = materializer_vnode:get_cache_name(Partition, pvc_snapshot_cache),
    StateReplica = clocksi_vnode:get_cache_name(Partition, pvc_state_table),
    %% TODO(borja): Change name of ETS table
    Committed = clocksi_vnode:get_cache_name(Partition, committed_tx),

    Self = generate_replica_name(node(), Partition, Id),

    {ok, #state{self=Self,
                partition = Partition,
                vlog_replica = VLog,
                committed_cache_replica = Committed,
                partition_state_replica = StateReplica}}.

handle_call(shutdown, _From, State) ->
    {stop, shutdown, ok, State};

handle_call(_Request, _From, _State) ->
    erlang:error(not_implemented).

handle_cast(_Request, _State) ->
    erlang:error(not_implemented).

%% Internal

-spec generate_replica_name(node(), partition_id(), non_neg_integer()) -> binary().
generate_replica_name(Node, Partition, Id) ->
    BinId = integer_to_binary(Id),
    BinPart = integer_to_binary(Partition),
    BinNode = atom_to_binary(Node, latin1),
    <<BinId/binary, "=", BinPart/binary, "=", BinNode/binary>>.

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

-spec all_ready([{partition_id(), node()}]) -> boolean().
all_ready([]) ->
    true;

all_ready([Node | Rest]) ->
    try
        %% TODO(borja): Change this to PVC storage node once done
        Res = riak_core_vnode_master:sync_command(Node,
                                                  pvc_check_servers_ready,
                                                  ?CLOCKSI_MASTER,
                                                  infinity),

        case Res of
            false -> false;
            true ->
                all_ready(Rest)
        end
    catch _:_  ->
        false
    end.

%% Unused

handle_info(Info, State) ->
    lager:info("Unhandled msg ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) -> ok.
code_change(_OldVsn, State, _Extra) -> {ok, State}.
