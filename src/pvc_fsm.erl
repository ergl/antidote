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

-module(pvc_fsm).

-behavior(gen_fsm).

%% For type partition_id, index_node, key, val
-include("antidote.hrl").
-include("pvc.hrl").

%% supervisor callback
-export([start_link/1]).

%% states
-export([client_command/3,
         read_result/2,
         read_batch_result/2,
         decide_vote/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-export_type([ws/0,
              partitions/0]).

-record(state, {
    %% Client PID
    from :: pid(),
    %% Active Transaction Object
    transaction :: txn(),
    %% Read accumulator for batch reads
    %% Contains the list of keys that have been requested
    %% When a key is read, it is swapped for its value
    read_accumulator = [] :: [key() | val()],
    %% Keys to be read (batch reads)
    read_remaining = [] :: [key()],
    %% Mapping of keys -> values.
    %% Used to cache updates for future reads
    writeset = orddict:new() :: ws(),
    %% Set of updated partitions, with their appropiate per-partition writeset
    updated_partitions = orddict:new() :: partitions(),
    %% Reason to abort
    abort_reason = undefined :: atom(),
    %% Partitions to Ack
    num_to_ack = 0 :: non_neg_integer(),
    %% Commit status
    ack_outcome = undefined :: undefined | {ok, pvc_vc()} | {error, reason()},
    %% Mapping of partitions -> keys
    %% Used to buffer the list of keys that must be indexed
    index_buffer = orddict:new() :: index_buffer()
}).

%% internal types

-type fsm_state() :: #state{}.
-type step(T) :: {next_state, T, fsm_state()}.
-type stop() :: {stop, normal, fsm_state()}.

%% External API

-spec start_link(pid()) -> {ok, pid()}.
start_link(From) ->
    gen_fsm:start_link(?MODULE, [From], []).

%% Init

init([From]) ->
    %% Seed random as we will probably have to choose a random read
    _ = rand:seed(exsplus, {erlang:phash2([node()]),
                            erlang:monotonic_time(),
                            erlang:unique_integer()}),

    TxId = #txn_id{server_pid=self()},
    Transaction = #txn{id=TxId,
                       has_read=sets:new(),
                       vc_dep=pvc_vclock:new(),
                       vc_aggr=pvc_vclock:new()},

    From ! {ok, TxId},
    {ok, client_command, #state{from=From,
                                transaction=Transaction}}.

%% States

-spec client_command(term(), pid(), fsm_state()) -> step(_) | stop().
client_command({unsafe_load, NKeys, Size}, Sender, State) ->
    unsafe_load_internal(NKeys, Size, State#state{from=Sender});

client_command({index, Keys}, Sender, State) ->
    index_internal(Keys, State#state{from=Sender});

client_command({scan_range, Root, Range, Limit}, Sender, State) ->
    scan_range_internal(Root, Range, Limit, State#state{from=Sender});

client_command({read, Key}, Sender, State) ->
    read_internal(Key, State#state{from=Sender});

client_command({read_batch, Keys}, Sender, State) ->
    read_batch_internal(Keys, State#state{from=Sender});

client_command({update, Key, Value}, Sender, State) ->
    update_internal(Key, Value, State#state{from=Sender});

client_command({update_batch, Updates}, Sender, State) ->
    update_batch_internal(Updates, State#state{from=Sender});

client_command(commit, Sender, State) ->
    commit(State#state{from=Sender});

client_command(_, _Sender, _State) ->
    erlang:error(undefined).

-spec read_result(term(), fsm_state()) -> step(_) | stop().
read_result({error, maxvc_bad_vc}, State) ->
    read_abort(State#state{abort_reason = pvc_bad_vc});

read_result({readreturn, From, _Key, Value, VCdep, Vcaggr}, State=#state{transaction=Tx}) ->
    gen_fsm:reply(State#state.from, {ok, Value}),
    {next_state, client_command, State#state{transaction=update_transaction(From, VCdep, Vcaggr, Tx)}}.

-spec read_batch_result(term(), fsm_state()) -> step(read_batch_result | client_command) | stop().
read_batch_result({error, maxvc_bad_vc}, State) ->
    read_abort(State#state{abort_reason=pvc_bad_vc});

read_batch_result({key_was_updated, Key, Value}, State) ->
    read_batch_loop(Key, Value, State);

read_batch_result({readreturn, From, Key, Value, VCdep, Vcaggr}, State=#state{transaction=Tx}) ->
    read_batch_loop(Key, Value, State#state{transaction=update_transaction(From, VCdep, Vcaggr, Tx)}).

-spec decide_vote({vote, partition_id(), term()}, fsm_state()) -> stop().
decide_vote({vote, From, Vote}, State = #state{num_to_ack=NAck,
                                               ack_outcome=AccOutcome}) ->

    case Vote of
        {error, Reason} ->
            decide(State#state{num_to_ack=0, ack_outcome = {error, Reason}});

        {ok, SeqNumber} ->
            {ok, PrevCommitVC} = AccOutcome,
            CommitVC = pvc_vclock:set_time(From, SeqNumber, PrevCommitVC),
            NewState = State#state{ack_outcome={ok, CommitVC}},
            case NAck > 1 of
                true ->
                    {next_state, decide_vote, NewState#state{num_to_ack=NAck - 1}};
                false ->
                    decide(NewState)
            end
    end.

%% internal

-spec update_transaction(partition_id(), pvc_vc(), pvc_vc(), txn()) -> txn().
update_transaction(From, VCdep, Vcaggr, Tx = #txn{
    has_read = HasRead,
    vc_aggr = T_VCaggr,
    vc_dep = T_VCdep
}) ->

    NewHasRead =  sets:add_element(From, HasRead),
    NewVCdep = pvc_vclock:max(T_VCdep, VCdep),
    NewVCaggr = pvc_vclock:max(T_VCaggr, Vcaggr),

    Tx#txn{vc_dep=NewVCdep,
           vc_aggr=NewVCaggr,
           has_read=NewHasRead}.

-spec unsafe_load_internal(non_neg_integer(), non_neg_integer(), fsm_state()) -> stop().
unsafe_load_internal(NKeys, Size, State) ->
    Val = crypto:strong_rand_bytes(Size),
    NewState = unsafe_loop(NKeys, Val, State),
    commit(NewState).

unsafe_loop(0, _, State) ->
    State;

unsafe_loop(N, Val, State=#state{updated_partitions=UpdatedPartitions}) ->
    Key = integer_to_binary(N, 36),
    Partition = log_utilities:get_key_partition(Key),
    WS = get_partition_writeset(Partition, UpdatedPartitions),
    NewWriteSet = orddict:store(Key, Val, WS),
    NewUpdatedPartitions = orddict:store(Partition, NewWriteSet, UpdatedPartitions),
    NewState = State#state{updated_partitions=NewUpdatedPartitions},
    unsafe_loop(N - 1, Val, NewState).

%% @doc Add the list of given keys to the index buffer
-spec index_internal([key()], fsm_state()) -> step(client_command).
index_internal(Keys, State=#state{from=Sender,
                                  index_buffer=IndexBuffer}) ->
    gen_fsm:reply(Sender, ok),

    NewIndexBuffer = lists:foldl(fun(Key, AccBuffer) ->
        P = log_utilities:get_key_partition(Key),
        KeySet = case orddict:find(P, AccBuffer) of
            error -> ordsets:new();
            {ok, S} -> S
        end,
        lager:info("Adding ~p to the keyset", [Key]),
        orddict:store(P, ordsets:add_element(Key, KeySet), AccBuffer)
    end, IndexBuffer, Keys),
    {next_state, client_command, State#state{index_buffer=NewIndexBuffer}}.

%% @doc Perform a range scan starting from Root with limit `Limit`
-spec scan_range_internal(key(), pvc_indices:range(), non_neg_integer(), fsm_state()) -> step(client_command).
scan_range_internal(Root, Range, Limit, State = #state{from=Sender,
                                                       index_buffer=IndexBuffer}) ->

    RootPartition = log_utilities:get_key_partition(Root),
    %% Get the matching keys in the locally updated set, if any
    LocalRange = get_local_range(RootPartition, Root, Range, IndexBuffer),

    %% Also get the matching keys stored in the ordered storage
    StoredRange = pvc_storage_vnode:read_range(RootPartition, Root, Range, Limit),

    %% Merge the two ranges (StoredRange is a normal list, not ordered)
    MergedRange = lists:foldl(fun ordsets:add_element/2, LocalRange, StoredRange),
    gen_fsm:reply(Sender, {ok, ordsets:to_list(MergedRange)}),
    {next_state, client_command, State}.

%% @doc Return the locally updated keys belonging to Partition that match
%%      the given prefix, skipping the root key
-spec get_local_range(index_node(), key(), pvc_indices:range(), index_buffer()) -> ordsets:ordset(key()).
get_local_range(Partition, Root, Range, IndexBuffer) ->
    case orddict:find(Partition, IndexBuffer) of
        error -> ordsets:new();
        {ok, KeySet} ->
            ordsets:fold(fun(Key, Acc) ->
                case Key of
                    Root -> Acc; %% Skip the root key
                    _ -> case pvc_indices:in_range(Key, Range) of
                        true ->
                            ordsets:add_element(Key, Acc);
                        false ->
                            Acc
                    end
                end
            end, ordsets:new(), KeySet)
    end.

-spec read_internal(key(), fsm_state()) -> step(read_result | client_command).
read_internal(Key, State=#state{from=Sender,
                                writeset=WS,
                                transaction=Transaction}) ->
    case orddict:find(Key, WS) of
        error ->
            HasRead = Transaction#txn.has_read,
            VCaggr = Transaction#txn.vc_aggr,
            pvc_read_replica:async_read(Key, HasRead, VCaggr),
            {next_state, read_result, State};

        {ok, Value} ->
            gen_fsm:reply(Sender, {ok, Value}),
            {next_state, client_command, State}
    end.

-spec read_batch_internal(key(), fsm_state()) -> step(read_batch_result).
read_batch_internal([Key | Rest]=Keys, State=#state{transaction=Transaction,writeset=WS}) ->
    ok = perform_read_batch(Key, Transaction, WS),
    {next_state, read_batch_result, State#state{read_accumulator=Keys,
                                                read_remaining=Rest}}.

-spec perform_read_batch(key(), txn(), ws()) -> ok.
perform_read_batch(Key, Transaction, WriteSet) ->
    case orddict:find(Key, WriteSet) of
        error ->
            HasRead = Transaction#txn.has_read,
            VCaggr = Transaction#txn.vc_aggr,
            pvc_read_replica:async_read(Key, HasRead, VCaggr);
        {ok, Value} ->
            gen_fsm:send_event(self(), {key_was_updated, Key, Value})
    end.

-spec read_batch_loop(key(), val(), fsm_state()) -> step(client_command | read_batch_result).
read_batch_loop(Key, Value, State=#state{from=Sender,
                                         writeset=WS,
                                         transaction=Transaction,
                                         read_accumulator=ReadAcc,
                                         read_remaining=RemainingKeys}) ->

    %% Replace the key with the value inside ReadAcc
    ReadValues = replace(ReadAcc, Key, Value),
    case RemainingKeys of
        [] ->
            gen_fsm:reply(Sender, {ok, ReadValues}),
            {next_state, client_command, State#state{read_accumulator=[]}};
        [NextKey | Rest] ->
            ok = perform_read_batch(NextKey, Transaction, WS),
            {next_state, read_batch_result, State#state{read_remaining=Rest,
                                                        read_accumulator=ReadValues}}
    end.

%% @doc Replaces the first occurrence of a key
%%
%%      Errors if the key is not present in the list
%%
-spec replace([key() | val()], key(), val()) -> [key() | val()] | error.
replace([], _, _) ->
    error;

replace([Key | Rest], Key, Value) ->
    [Value | Rest];

replace([OtherKey | Rest], Key, Value) ->
    [OtherKey | replace(Rest, Key, Value)].

-spec update_internal(key(), val(), fsm_state()) -> step(client_command).
update_internal(Key, Value, State=#state{from=Sender,
                                         writeset=WS,
                                         updated_partitions=UpdatedPartitions}) ->
    gen_fsm:reply(Sender, ok),

    {NewWriteSet, NewPartitions} = update_state(Key, Value, WS, UpdatedPartitions),
    {next_state, client_command, State#state{writeset=NewWriteSet,
                                             updated_partitions=NewPartitions}}.

-spec update_batch_internal([{key(), val()}], fsm_state()) -> step(client_command).
update_batch_internal(Updates, State=#state{from=Sender}) ->

    gen_fsm:reply(Sender, ok),

    NewState = lists:foldl(fun({Key, Value}, Acc) ->
        {NewWriteSet, NewPartitions} = update_state(Key,
                                                    Value,
                                                    Acc#state.writeset,
                                                    Acc#state.updated_partitions),

        Acc#state{writeset=NewWriteSet, updated_partitions=NewPartitions}
    end, State, Updates),

    {next_state, client_command, NewState}.

-spec commit(fsm_state()) -> step(decide_vote) | stop().
commit(State=#state{from=From,
                    transaction=Transaction,
                    updated_partitions=Partitions}) ->

    case Partitions of
        [] ->
            gen_fsm:reply(From, ok),
            {stop, normal, State};

        _ ->
            Ack = orddict:size(Partitions),
            TxId = Transaction#txn.id,
            CommitVC = Transaction#txn.vc_dep,
            ok = pvc_storage_vnode:prepare(Partitions, TxId, CommitVC),
            {next_state, decide_vote, State#state{num_to_ack=Ack, ack_outcome={ok, CommitVC}}}
    end.

-spec decide(fsm_state()) -> stop().
decide(State=#state{from=Sender,
                    ack_outcome=Outcome,
                    transaction=Transaction,
                    index_buffer=IndexBuffer,
                    updated_partitions=UpdatedPartitions}) ->

    OutcomeMsg = case Outcome of
        {ok, _} -> ok;
        Err -> Err
    end,
    gen_fsm:reply(Sender, OutcomeMsg),

    TxId = Transaction#txn.id,
    %% Merge the partition updates and the partition keyset
    %% We don't use orddict:merge/3 because it only combines
    %% keys that exist in both dictionaries. We might not hold
    %% indices for all the partitions that have been updated.
    PartitionsWithIndex = orddict:map(fun(Partition, _) ->
        case orddict:find(Partition, IndexBuffer) of
            error -> [];
            {ok, KeySet} -> KeySet
        end
    end, UpdatedPartitions),
    ok = pvc_storage_vnode:decide(PartitionsWithIndex, TxId, Outcome),
    {stop, normal, State}.

-spec read_abort(fsm_state()) -> stop().
read_abort(State=#state{from=From,
                        abort_reason=Reason}) ->

    gen_fsm:reply(From, {error, Reason}),
    {stop, normal, State}.

%% Util functions

%% @doc Util function to update the FSM state with a new client update
-spec update_state(key(), val(), ws(), partition_id()) -> {ws(), partitions()}.
update_state(Key, Value, WriteSet, UpdatedPartitions) ->
    Partition = log_utilities:get_key_partition(Key),
    NewWriteSet = orddict:store(Key, Value, WriteSet),
    PartitionWS = get_partition_writeset(Partition, UpdatedPartitions),
    %% Store the per-partition WriteSet
    NewUpdatedPartitions = orddict:store(Partition,
                                         orddict:store(Key, Value, PartitionWS),
                                         UpdatedPartitions),
    {NewWriteSet, NewUpdatedPartitions}.

%% @doc Util function to implement a default-valued orddict:find
-spec get_partition_writeset(index_node(), partitions()) -> ws().
get_partition_writeset(Partition, UpdatedPartitions) ->
    case orddict:find(Partition, UpdatedPartitions) of
        error ->
            [];
        {ok, Value} ->
            Value
    end.

%% gen_fsm mock callbacks
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.
terminate(_Reason, _SN, _SD) -> ok.
handle_info(_Info, _StateName, StateData) -> {stop, badmsg, StateData}.
handle_event(_Event, _StateName, StateData) -> {stop, badmsg, StateData}.
handle_sync_event(stop, _From, _StateName, StateData) -> {stop, normal, ok, StateData};
handle_sync_event(_Event, _From, _StateName, StateData) -> {stop, badmsg, StateData}.
