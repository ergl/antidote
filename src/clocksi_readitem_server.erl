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
    BinId = binary:encode_unsigned(Id),
    BinPart = binary:encode_unsigned(Partition),
    BinNode = atom_to_binary(Node, latin1),
    Name = <<BinId/binary, BinPart/binary, BinNode/binary>>,
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

    MatState = #mat_state{is_ready=false,
                          partition=Partition,
                          ops_cache=OpsCache,
                          snapshot_cache=SnapshotCache},

    %% ClockSI Cache
    PreparedCache = clocksi_vnode:get_cache_name(Partition, prepared),

    Self = generate_server_name(Addr, Partition, Id),
    {ok, #state{id=Id,
                self=Self,
                partition=Partition,
                mat_state = MatState,
                prepared_cache=PreparedCache}}.

handle_call({perform_read, Key, Type, Transaction}, Coordinator, SD0) ->
    ok = perform_read_internal(Coordinator, Key, Type, Transaction, [], SD0),
    {noreply, SD0};

handle_call(go_down, _Sender, SD0) ->
    {stop, shutdown, ok, SD0}.

handle_cast({perform_read_cast, Coordinator, Key, Type, Transaction}, SD0) ->
    ok = perform_read_internal(Coordinator, Key, Type, Transaction, [], SD0),
    {noreply, SD0}.

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
