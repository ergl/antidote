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
-module(antidote_pvc_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include("pvc.hrl").
-include("debug_log.hrl").

%% Public API
-export([most_recent_vc/1,
         vnodes_ready/0,
         replicas_ready/0]).

%% Protocol API
-export([prepare/5,
         decide/3]).

%% riak_core_vnode callbacks
-export([start_vnode/1,
         init/1,
         handle_command/3,
         handle_coverage/4,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_command/3,
         handle_handoff_data/2,
         encode_handoff_item/2,
         is_empty/1,
         terminate/2,
         handle_exit/3,
         delete/1]).

-type protocol() :: psi | ser.
-type tx_id() :: term().
-type decision() :: abort | {ready, pvc_vc()}.
-type cache(_K, _V) :: ets:tab().

-define(VNODE_MASTER, antidote_pvc_vnode_master).
-define(WRITE_HEAVY_OPTS, [set, public, named_table, {write_concurrency, true}]).
-define(PENDING_OPTS, [bag, public, named_table, {read_concurrency, true}, {write_concurrency, true}]).

-record(psi_payload, {
    writeset :: #{key() => val()}
}).

-record(ser_payload, {
    writeset :: #{key() => val()},
    readset :: #{key() => non_neg_integer()}
}).

-record(psi_data, {
    write_keys :: [key()],
    writeset :: #{key() => val()}
}).

-record(ser_data, {
    write_keys :: [key()],
    writeset :: #{key() => val()},
    readset :: [{key(), non_neg_integer()}]
}).

-type protocol_payload() :: #psi_payload{} | #ser_payload{}.
-type persist_data() :: #psi_data{} | #ser_data{}.

%% Called internally by riak_core, or via rpc
-ignore_xref([start_vnode/1,
              replicas_ready/0,
              vnodes_ready/0]).

-record(state, {
    partition :: partition_id(),

    %% number of gen_servers replicating this vnode state
    replicas_n = ?READ_CONCURRENCY :: non_neg_integer(),
    %% Most Recent VC, cached in an ETS
    most_recent_vc :: cache(mrvc, pvc_vc()),

    %% Cache the last committed version of a key
    last_vsn_cache :: cache(key(), non_neg_integer()),
    %% If a key is not present in the above cache, return this
    default_last_vsn = 0 :: non_neg_integer(),

    %% Last prepared counter, increased on each prepare
    last_prepared = 0 :: non_neg_integer(),
    %% Commit queue of pending transactions
    commit_queue = pvc_commit_queue:new() :: pvc_commit_queue:cqueue(),

    %% Holds temporary data for the transactions while they sit
    %% on the commit queue. Used to offload memory to an ETS.
    pending_tx_data :: cache(tx_id(), persist_data()),

    %% A pair of inverted indices over the transactions read/write sets
    %% Used to compute write-write and read-write conflicts in a quick way.
    pending_reads :: cache(key(), tx_id()),
    pending_writes :: cache(key(), tx_id()),

    %% Contains the decisions that clients make about
    %% transactions after a voting phase. Client will write to this table
    %% from outside of the virtual node, to avoid serialization overhead.
    decision_cache :: cache(tx_id(), {protocol(), decision()}),

    dequeue_timer = undefined :: timer:tref() | undefined
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec most_recent_vc(partition_id()) -> pvc_vc().
most_recent_vc(Partition) ->
    ets:lookup_element(cache_name(Partition, ?MRVC_TABLE), mrvc, 2).

%% @doc For all nodes in the system, report if all the vnodes are ready
-spec vnodes_ready() -> boolean().
vnodes_ready() ->
    Nodes = dc_utilities:get_all_partitions_nodes(),
    ReadyFun = fun(IndexNode) ->
        try
            riak_core_vnode_master:sync_command(IndexNode,
                                                is_ready,
                                                ?VNODE_MASTER,
                                                infinity)
        catch _:_ -> false
        end
    end,
    lists:all(ReadyFun, Nodes).

%% @doc For all nodes in the system, report if the read replicas are online
-spec replicas_ready() -> boolean().
replicas_ready() ->
    Nodes = dc_utilities:get_all_partitions_nodes(),
    ReadyFun = fun(IndexNode) ->
        try
            riak_core_vnode_master:sync_command(IndexNode,
                                                replicas_ready,
                                                ?VNODE_MASTER,
                                                infinity)
        catch _:_ -> false
        end
    end,
    lists:all(ReadyFun, Nodes).

-spec prepare(Partition :: partition_id(),
              Protocol :: protocol(),
              TxId :: tx_id(),
              Payload :: term(),
              Version :: non_neg_integer()) -> {ok, non_neg_integer()} | {error, reason()}.

prepare(Partition, Protocol, TxId, Payload, Version) ->
    riak_core_vnode_master:sync_command({Partition, node()},
                                        {prepare, TxId, make_payload(Protocol, Payload), Version},
                                        ?VNODE_MASTER,
                                        infinity).

-spec decide(Partition :: partition_id(),
             TxId :: tx_id(),
             Outcome :: decision()) -> ok.

decide(Partition, TxId, Outcome) ->
    decide_internal(Partition, TxId, Outcome).

%%%===================================================================
%%% riak_core callbacks
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    MRVC = new_cache(Partition, ?MRVC_TABLE),
    true = ets:insert(MRVC, {mrvc, pvc_vclock:new()}),

    State = #state{
        partition       = Partition,
        most_recent_vc  = MRVC,
        last_vsn_cache  = new_cache(Partition, ?LAST_VSN_TABLE),
        pending_tx_data = new_cache(Partition, ?PENDING_DATA_TABLE, ?WRITE_HEAVY_OPTS),
        pending_reads   = new_cache(Partition, ?PENDING_READS_TABLE, ?PENDING_OPTS),
        pending_writes  = new_cache(Partition, ?PENDING_WRITES_TABLE, ?PENDING_OPTS),
        decision_cache  = new_cache(Partition, ?DECIDE_TABLE, ?WRITE_HEAVY_OPTS)
    },

    {ok, State}.

%% Used by antidote to check that vnode is alive
handle_command({hello}, _Sender, State) ->
    {reply, ok, State};

handle_command(is_ready, _Sender, State) ->
    Ready = lists:all(fun is_ready/1, [State#state.last_vsn_cache,
                                       State#state.pending_tx_data,
                                       State#state.pending_reads,
                                       State#state.pending_writes,
                                       State#state.decision_cache]),
    {reply, Ready, State};

handle_command(start_replicas, _From, S = #state{partition=P, replicas_n=N}) ->
    ok = pvc_read_replica:start_replicas(P, N),
    Result = pvc_read_replica:replica_ready(P, N),
    {ok, TRef} = schedule_dequeue_interval(P),
    {reply, Result, S#state{dequeue_timer=TRef}};

handle_command(stop_replicas, _From, S = #state{partition=P, replicas_n=N, dequeue_timer=TRef}) ->
    ok = pvc_read_replica:stop_replicas(P, N),
    ok = stop_dequeue_interval(TRef),
    {reply, ok, S#state{dequeue_timer=undefined}};

handle_command(replicas_ready, _From, S = #state{partition=P, replicas_n=N}) ->
    Result = pvc_read_replica:replica_ready(P, N),
    {reply, Result, S};

handle_command(refresh_replicas, _From, S = #state{partition=P, replicas_n=N}) ->
    Result = pvc_read_replica:refresh_default(P, N),
    {reply, Result, S};

handle_command({unsafe_set_clock, Seq, MRVC}, _From, S = #state{partition=P, most_recent_vc=MRVCTable}) ->
    ok = logging_vnode:pvc_insert_to_commit_log(P, MRVC),
    true = ets:insert(MRVCTable, {mrvc, MRVC}),
    {reply, ok, S#state{default_last_vsn=Seq, last_prepared=Seq}};

handle_command(flush_queue, _From, State) ->
    lists:foreach(fun ets:delete_all_objects/1, [State#state.pending_tx_data,
                                                 State#state.pending_reads,
                                                 State#state.pending_writes,
                                                 State#state.decision_cache]),

    {reply, ok, State#state{commit_queue=pvc_commit_queue:new()}};

handle_command({prepare, TxId, Payload, Version}, _From, State) ->
    ?LAGER_LOG("{prepare, ~p, ~p, ~p}", [TxId, Payload, Version]),
    {Reply, NewState} = prepare_internal(TxId, Payload, Version, State),
    {reply, Reply, NewState};

handle_command(dequeue_event, _From, State) ->
    NewQueue = dequeue_event_internal(State),
    {noreply, State#state{commit_queue=NewQueue}}.

%%%===================================================================
%%% Prepare Internal Functions
%%%===================================================================

-spec prepare_internal(TxId :: tx_id(),
                       Payload :: protocol_payload(),
                       Version :: non_neg_integer(),
                       State :: #state{}) -> {term(), #state{}}.

prepare_internal(TxId, Payload, Version, State = #state{commit_queue=Queue,
                                                        last_prepared=LastPrep}) ->
    Valid = conflict_check(Payload, Version, State),
    case Valid of
        {error, _}=Err ->
            ?LAGER_LOG("prepare @ ~p = ~p", [State#state.partition, Err]),
            lager:info("[~p] PREPARE ABORT ~p", [TxId, State#state.partition]),
            {Err, State};

        {ok, PersistData} ->
            Seq = LastPrep + 1,
            NewQueue = pvc_commit_queue:enqueue(TxId, Queue),
            ok = persist_data(TxId, PersistData, State),

            ?LAGER_LOG("prepare @ ~p = ~p", [State#state.partition, {ok, Seq}]),
            lager:info("[~p] PREPARE PENDING ~p", [TxId, State#state.partition]),
            Reply = {ok, Seq},
            NewState = State#state{last_prepared=Seq, commit_queue=NewQueue},
            {Reply, NewState}
    end.

-spec conflict_check(Payload :: protocol_payload(),
                     Version :: non_neg_integer(),
                     State :: #state{}) -> {ok, persist_data()} | {error, reason()}.

conflict_check(Payload=#ser_payload{}, Version, #state{pending_reads=Reads,
                                                       pending_writes=Writes,
                                                       last_vsn_cache=LastVsnCache,
                                                       default_last_vsn=DefaultVsn}) ->
    Data = make_data(Payload),
    ValidReadset = valid_readset(Data, Writes, LastVsnCache, DefaultVsn),
    case ValidReadset of
        {error, _}=ReadErr ->
            ReadErr;
        true ->
            ValidWriteset = valid_writeset(Data, Reads, Writes, Version, LastVsnCache, DefaultVsn),
            case ValidWriteset of
                {error, _}=WriteErr ->
                    WriteErr;
                true ->
                    {ok, Data}
            end
    end;

conflict_check(Payload=#psi_payload{}, Version, #state{pending_reads=Reads,
                                                       pending_writes=Writes,
                                                       last_vsn_cache=LastVsnCache,
                                                       default_last_vsn=DefaultVsn}) ->
    Data = make_data(Payload),
    ValidWriteset = valid_writeset(Data, Reads, Writes, Version, LastVsnCache, DefaultVsn),
    case ValidWriteset of
        {error, _}=WriteErr ->
            WriteErr;
        true ->
            {ok, Data}
    end.

%%%===================================================================
%%% Prepare Conflict Check Functions
%%%===================================================================

-spec valid_readset(Data :: #ser_data{},
                    Writes :: cache(key(), tx_id()),
                    LastVsnCache :: cache(key(), non_neg_integer()),
                    DefaultVsn :: non_neg_integer()) -> true | {error, reason()}.

valid_readset(#ser_data{readset=Keys}, Writes, LastVsnCache, DefaultVsn) ->
    ConflictFunction = fun(Key, Version) ->
        check_key_overlap(Key, Version, Writes, LastVsnCache, DefaultVsn)
    end,
    valid_readset_inernal(Keys, ConflictFunction).

-spec valid_writeset(Data :: persist_data(),
                     Reads :: cache(key(), tx_id()),
                     Writes :: cache(key(), tx_id()),
                     Version :: non_neg_integer(),
                     LastVsnCache :: cache(key(), non_neg_integer()),
                     DefaultVsn :: non_neg_integer()) -> true | {error, reason()}.

valid_writeset(#ser_data{write_keys=WKeys}, Reads, Writes, Version, LastVsnCache, DefaultVsn) ->
    ConflictFun = fun(Key) ->
    Disputed = ets:member(Reads, Key) orelse
               ets:member(Writes, Key),
    case Disputed of
        true -> {error, pvc_conflict};
        false -> stale_version(Key, Version, LastVsnCache, DefaultVsn)
    end
    end,
    valid_writeset_internal(WKeys, ConflictFun);

valid_writeset(#psi_data{write_keys=WKeys}, _Reads, Writes, Version, LastVsnCache, DefaultVsn) ->
    ConflictFun = fun(Key) ->
        check_key_overlap(Key, Version, Writes, LastVsnCache, DefaultVsn)
    end,
    valid_writeset_internal(WKeys, ConflictFun).

-spec persist_data(TxId :: tx_id(), Data :: persist_data(), State :: #state{}) -> ok.
persist_data(TxId, Data=#psi_data{write_keys=Keys}, State) ->
    true = ets:insert(State#state.pending_tx_data, {TxId, Data}),
    true = ets:insert(State#state.pending_writes, [{Key, TxId} || Key <- Keys]),
    ok;

persist_data(TxId, Data=#ser_data{write_keys=WKeys, readset=RS}, State) ->
    true = ets:insert(State#state.pending_tx_data, {TxId, Data}),
    true = ets:insert(State#state.pending_writes, [{Key, TxId} || Key <- WKeys]),
    true = ets:insert(State#state.pending_reads, [{Key, TxId} || {Key, _} <- RS]),
    ok.

%%%===================================================================
%%% Prepare Util Functions
%%%===================================================================

-spec make_payload(protocol(), term()) -> protocol_payload().
make_payload(psi, WS) -> #psi_payload{writeset=WS};
make_payload(ser, {RS, WS}) -> #ser_payload{readset=RS, writeset=WS}.

%% @doc Memoize keys so we don't need to compute them each time
-spec make_data(protocol_payload()) -> persist_data().
make_data(#ser_payload{writeset=WS, readset=RS}) ->
    #ser_data{writeset=WS,
              readset=maps:to_list(RS),
              write_keys=maps:keys(WS)};

make_data(#psi_payload{writeset=WS}) ->
    #psi_data{writeset=WS,
              write_keys=maps:keys(WS)}.

-spec check_key_overlap(Key :: key(),
                        Version :: non_neg_integer(),
                        Writes :: cache(key(), tx_id()),
                        LastVsnCache :: cache(key(), non_neg_integer()),
                        DefaultVsn :: non_neg_integer()) -> true | {error, reason()}.

check_key_overlap(Key, Version, Writes, LastVsnCache, DefaultVsn) ->
    Disputed = ets:member(Writes, Key),
    case Disputed of
        true -> {error, pvc_conflict};
        false -> stale_version(Key, Version, LastVsnCache, DefaultVsn)
    end.

-spec stale_version(Key :: key(),
                    Version :: non_neg_integer(),
                    LastVsnCache :: cache(key(), non_neg_integer()),
                    DefaultVsn :: non_neg_integer()) -> true | {error, reason()}.

stale_version(Key, Version, LastVsnCache, DefaultVsn) ->
    StoredVersion = try ets:lookup_element(LastVsnCache, Key, 2) catch _:_ -> DefaultVsn end,
    Stale = StoredVersion > Version,
    case Stale of
        true ->
            {error, pvc_stale_tx};
        false ->
            true
    end.

-spec valid_readset_inernal(
    Versions :: [{key(), non_neg_integer()}],
    ConflictFun :: fun((key(), non_neg_integer()) -> true | {error, reason()})
) -> true | {error, reason()}.

valid_readset_inernal([], _Fun) -> true;
valid_readset_inernal([{Key, Vsn} | Rest], ConflictFun) ->
    case ConflictFun(Key, Vsn) of
        {error, _}=Err -> Err;
        true -> valid_readset_inernal(Rest, ConflictFun)
    end.

-spec valid_writeset_internal(
    Versions :: [{key(), non_neg_integer()}],
    ConflictFun :: fun((key()) -> true | {error, reason()})
) -> true | {error, reason()}.

valid_writeset_internal([], _Fun) -> true;
valid_writeset_internal([Key | Rest], ConflictFun) ->
    case ConflictFun(Key) of
        {error, _}=Err -> Err;
        true -> valid_writeset_internal(Rest, ConflictFun)
    end.

%%%===================================================================
%%% Decide Internal Functions
%%%===================================================================

-spec decide_internal(Partition :: partition_id(),
                      TxId :: tx_id(),
                      Outcome :: {ok, pvc_vc()} | abort) -> ok.

decide_internal(Partition, TxId, {ok, VC}) ->
    ?LAGER_LOG("Commit: Mark ~p as ready", [TxId]),
    lager:info("[~p] DECIDE READY ~p", [TxId, Partition]),
    ets:insert(cache_name(Partition, ?DECIDE_TABLE), {TxId, ready, VC}),
    ok;

decide_internal(Partition, TxId, abort) ->
    ?LAGER_LOG("Abort: Removing ~p from CommitQueue", [TxId]),
    case ets:take(cache_name(Partition, ?PENDING_DATA_TABLE), TxId) of
        [{TxId, Payload}] ->
            lager:info("[~p] DECIDE ABORT ~p", [TxId, Partition]),
            true = ets:insert(cache_name(Partition, ?DECIDE_TABLE), {TxId, abort}),
            ok = clear_pending(Partition, TxId, Payload);
        _ ->
            lager:info("[~p] DECIDE DISCARD ~p", [TxId, Partition]),
            %% If this transaction was marked as aborted by this partition, no state will be kept,
            %% the transaction won't even be in the commit queue, so there's no need to store the
            %% decision in the decide table, otherwise we would have a memory leak
            ok
    end.

-spec clear_pending(Partition :: partition_id(),
                    TxId :: tx_id(),
                    Data :: persist_data()) -> ok.

clear_pending(Partition, TxId, #psi_data{write_keys=WKeys}) ->
    clear_writes(TxId, WKeys, cache_name(Partition, ?PENDING_WRITES_TABLE));

clear_pending(Partition, TxId, #ser_data{write_keys=WKeys, readset=RS}) ->
    clear_reads(TxId, RS, cache_name(Partition, ?PENDING_READS_TABLE)),
    clear_writes(TxId, WKeys, cache_name(Partition, ?PENDING_WRITES_TABLE)).

-spec clear_reads(tx_id(), [{key(), non_neg_integer()}], cache(key(), tx_id())) -> ok.
clear_reads(TxId, ReadKeys, Reads) ->
    _ = [ets:delete_object(Reads, {Key, TxId}) || {Key, _} <- ReadKeys],
    ok.

-spec clear_writes(tx_id(), [key()], cache(key(), tx_id())) -> ok.
clear_writes(TxId, WriteKeys, Writes) ->
    _ = [ets:delete_object(Writes, {Key, TxId}) || Key <- WriteKeys],
    ok.

%%%===================================================================
%%% Dequeue Functions
%%%===================================================================

-spec dequeue_event_internal(#state{}) -> pvc_commit_queue:cqueue().
dequeue_event_internal(#state{partition = Partition,
                              commit_queue = Queue,
                              most_recent_vc = MRVCTable,
                              decision_cache = DecideTable,
                              last_vsn_cache = LastVsnCache,
                              pending_tx_data = PendingData}) ->

    {ReadyTx, NewQueue} = pvc_commit_queue:dequeue_ready(Queue, DecideTable, PendingData),
    case ReadyTx of
        [] ->
            ok;
        Entries ->
            lists:foreach(fun({TxId, TxData, CommitVC}) ->
                lager:info("[~p] DEQUEUE ~p", [TxId, Partition]),

                ?LAGER_LOG("Processing ~p, update materializer", [TxId]),
                %% Persist the writeset in storage
                ok = update_materializer(Partition, TxData, CommitVC),

                %% Update current MRVC
                ?LAGER_LOG("Processing ~p, update MRVC", [TxId]),
                NewMRVC = update_mrvc(CommitVC, MRVCTable),

                %% Update Commit Log
                ?LAGER_LOG("Processing ~p, append to CLog", [TxId]),
                ok = logging_vnode:pvc_insert_to_commit_log(Partition, NewMRVC),

                ?LAGER_LOG("Processing ~p, cache VLog.last", [TxId]),
                CommitTime = pvc_vclock:get_time(Partition, CommitVC),
                ok = cache_last_vsn(TxData, CommitTime, LastVsnCache),

                %% Remove the keys from the pending table
                %% NOTE: Don't try to optimize by fusing this loop with the previous
                %% function. We want the changes to the VLog to be visible before
                %% removing the key from the pending table. Since Erlang doesn't offer
                %% an atomic multi-object ets:delete_object/2, we have to do it the old way.
                ok = clear_pending(Partition, TxId, TxData)
            end, Entries)
    end,
    NewQueue.

-spec update_materializer(Partition :: partition_id(),
                          TxData :: persist_data(),
                          CommitVC :: pvc_vc()) -> ok.

update_materializer(Partition, TxData, CommitVC) ->
    WS = case TxData of
        #ser_data{} -> TxData#ser_data.writeset;
        #psi_data{} -> TxData#psi_data.writeset
    end,
    materializer_vnode:pvc_update_keys(Partition, WS, CommitVC).

%% @doc Update the partition MostRecentVC and return its value
-spec update_mrvc(CommitVC :: pvc_vc(), MRVCTable :: cache(mrvc, pvc_vc())) -> pvc_vc().
update_mrvc(CommitVC, MRVCTable) ->
    OldMRVC = ets:lookup_element(MRVCTable, mrvc, 2),
    NewMRVC = pvc_vclock:max(CommitVC, OldMRVC),
    true = ets:update_element(MRVCTable, mrvc, {2, NewMRVC}),
    NewMRVC.

-spec cache_last_vsn(TxData :: persist_data(),
                     CommitTime :: non_neg_integer(),
                     LastVsnCache :: cache(key(), non_neg_integer())) -> ok.

cache_last_vsn(TxData, CommitTime, LastVsnCache) ->
    WKeys = case TxData of
        #ser_data{} -> TxData#ser_data.write_keys;
        #psi_data{} -> TxData#psi_data.write_keys
    end,
    Objects = [{Key, CommitTime} || Key <- WKeys],
    true = ets:insert(LastVsnCache, Objects),
    ok.

%%%===================================================================
%%% Util Functions
%%%===================================================================

-spec safe_bin_to_atom(binary()) -> atom().
safe_bin_to_atom(Bin) ->
    case catch binary_to_existing_atom(Bin, latin1) of
        {'EXIT', _} -> binary_to_atom(Bin, latin1);
        Atom -> Atom
    end.

-spec new_cache(partition_id(), atom()) -> cache_id().
new_cache(Partition, Name) ->
    new_cache(Partition, Name, [set, protected, named_table, ?TABLE_CONCURRENCY]).

new_cache(Partition, Name, Options) ->
    CacheName = cache_name(Partition, Name),
    case ets:info(CacheName) of
        undefined ->
            ets:new(CacheName, Options);
        _ ->
            lager:info("Unsable to create cache ~p at ~p, retrying", [Name, Partition]),
            timer:sleep(100),
            try ets:delete(CacheName) catch _:_ -> ok end,
            new_cache(Partition, Name, Options)
    end.

-spec cache_name(partition_id(), atom()) -> cache_id().
cache_name(Partition, Name) ->
    BinNode = atom_to_binary(node(), latin1),
    BiName = atom_to_binary(Name, latin1),
    BinPart = integer_to_binary(Partition),
    TableName = <<BiName/binary, <<"-">>/binary, BinPart/binary, <<"@">>/binary, BinNode/binary>>,
    safe_bin_to_atom(TableName).

-spec is_ready(cache_id()) -> boolean().
is_ready(Table) ->
    undefined =/= ets:info(Table).

%% @doc Schedule a dequeue event in the vnode
%%
%%      This function will send a message to the Partition's vnode every
%%      DEQUEUE_INTERVAL ms
-spec schedule_dequeue_interval(partition_id()) -> {ok, timer:tref()}.
schedule_dequeue_interval(Partition) ->
    Args = [{Partition, node()}, dequeue_event, ?VNODE_MASTER],
    timer:apply_interval(?DEQUEUE_INTERVAL, riak_core_vnode_master,
                                            command,
                                            Args).

-spec stop_dequeue_interval(timer:tref()) -> ok.
stop_dequeue_interval(TRef) ->
    {ok, cancel} = timer:cancel(TRef),
    ok.

%%%===================================================================
%%% stub riak_core callbacks
%%%===================================================================

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handoff_starting(_, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_, State) ->
    {ok, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_data(_Arg0, _Arg1) ->
    erlang:error(not_implemented).

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

delete(State) ->
    {ok, State}.
