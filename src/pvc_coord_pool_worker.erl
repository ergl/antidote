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
-module(pvc_coord_pool_worker).
-behavior(gen_fsm).
-behavior(poolboy_worker).

-include("antidote.hrl").

%% supervisor callback
-export([start_link/1]).

%% states
-export([wait_checkout/3,
         wait_checkin/3,
         client_command/3,
         read_result/2,
         read_batch_result/2,
         pvc_receive_votes/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {
    %% Client PID
    from = not_ready :: not_ready | pid(),
    %% Active Transaction Object
    transaction :: tx(),
    %% Read accumulator for batch reads
    %% Contains the list of keys that have been requested
    %% When a key is read, it is swapped for its value
    read_accumulator = [] :: [key() | val()],
    %% Keys to be read (batch reads)
    read_remaining = [] :: [key()],
    %% Both updated_partitions and client_ops serve as writesets
    %% TODO(borja): Merge into same field
    updated_partitions = [] :: list(),
    client_ops = [] :: list(),
    %% Reason to abort
    abort_reason = undefined :: atom(),
    %% Partitions to Ack
    num_to_ack = 0 :: non_neg_integer(),
    %% Commit status
    ack_outcome = undefined :: undefined | {ok, pvc_vc()} | {error, reason()},
    %% Mapping of partitions -> keys
    %% Used to buffer the list of keys that must be indexed
    index_buffer = dict:new() :: dict:dict(),

    tmp_read_map_stamps = #{}
}).

-spec start_link(pid()) -> {ok, pid()}.
start_link(Args) ->
    gen_fsm:start_link(?MODULE, Args, []).

%% Init

init([]) ->
    %% Seed random as we will probably have to choose a random read
    _ = rand:seed(exsplus, {erlang:phash2([node()]),
        erlang:monotonic_time(),
        erlang:unique_integer()}),

    {ok, wait_checkout, fresh_state(fresh_transaction())}.

%% States

%% @doc This message gets delivered by the pool when client performs check out
wait_checkout(checkout, From, State) ->
    gen_fsm:reply(From, ok),
    {next_state, client_command, State};

wait_checkout(_, From, State) ->
    gen_fsm:reply(From, {error, waiting_checkout}),
    {next_state, wait_checkout, State}.

%% @doc This message gets delivered by the pool when client performs check in
wait_checkin(checkin, From, State) ->
    gen_fsm:reply(From, ok),
    {next_state, wait_checkout, State};

wait_checkin(_, From, State) ->
    gen_fsm:reply(From, {error, waiting_checkin}),
    {next_state, wait_checkin, State}.

%% @doc UNSAFE: Execute a blind write of size Size to NKeys
client_command({unsafe_load, NKeys, Size}, Sender, State) ->
    unsafe_load_internal(NKeys, Size, State#state{from=Sender});

%% @doc Execute the commit protocol
client_command(commit, Sender, State) ->
    prepare_internal(State#state{from=Sender});

%% @doc Read a single key, asynchronous
client_command({read, Key}, Sender, State) ->
    read_internal(Key, State#state{from=Sender});

%% @doc Read a batch of objects, asynchronous
client_command({read_batch, Keys}, Sender, State) ->
    read_batch_internal(Keys, State#state{from=Sender});

client_command({update, Key, Value}, Sender, State) ->
    update_internal(Key, Value, State#state{from=Sender});

client_command({update_batch, UpdateOps}, Sender, State) ->
    update_batch_internal(UpdateOps, State#state{from=Sender});

client_command({pvc_index, Updates}, Sender, State = #state{
    index_buffer=ToIndexDict
}) ->
    gen_fsm:reply(Sender, ok),

    NewIndexSet = lists:foldl(fun({K, _}, Acc) ->
        P = log_utilities:get_key_partition(K),
        pvc_add_to_index_dict(P, K, Acc)
    end, ToIndexDict, Updates),
    {next_state, client_command, State#state{index_buffer=NewIndexSet}};

client_command({pvc_scan_range, Root, Range, Limit}, Sender, State = #state{
    index_buffer=ToIndexDict
}) ->
    Partition = log_utilities:get_key_partition(Root),

    %% Get the matching keys in the locally updated set, if any
    LocalIndexSet = get_local_range(Partition, Root, Range, ToIndexDict),

    %% Also get the matching keys stored in the ordered storage
    StoredIndexKeys = materializer_vnode:pvc_key_range(Partition, Root, Range, Limit),

    %% Merge them (don't store duplicates)
    MergedKeys = lists:foldl(fun ordsets:add_element/2, LocalIndexSet, StoredIndexKeys),
    gen_fsm:reply(Sender, {ok, ordsets:to_list(MergedKeys)}),
    {next_state, client_command, State}.

unsafe_load_internal(NKeys, Size, State) ->
    Val = crypto:strong_rand_bytes(Size),
    NewState = unsafe_loop(NKeys, Val, State),
    prepare_internal(NewState).

unsafe_loop(0, _, State) ->
    State;

unsafe_loop(N, Val, S = #state{client_ops=Ops0,updated_partitions=P0}) ->
    Key = integer_to_binary(N, 36),
    {P, Ops} = update_state(Key, Val, Ops0, P0),
    unsafe_loop(N - 1, Val, S#state{client_ops=Ops,updated_partitions=P}).

-spec pvc_add_to_index_dict(index_node(), key(), dict:dict()) -> dict:dict().
pvc_add_to_index_dict(Part, Key, Dict) ->
    S = case dict:find(Part, Dict) of
        error ->
            ordsets:new();
        {ok, Set} ->
            Set
    end,
    NewS = ordsets:add_element(Key, S),
    dict:store(Part, NewS, Dict).

%% @doc Return the locally updated keys belonging to Partition that match
%%      the given prefix, skipping the root key
-spec get_local_range(partition_id(), key(), pvc_indices:range(), dict:dict()) -> ordsets:ordset().
get_local_range(Partition, Root, Range, ToIndexDict) ->
    case dict:find(Partition, ToIndexDict) of
        error -> ordsets:new();
        {ok, S} ->
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
            end, ordsets:new(), S)
    end.

read_internal(Key, State = #state{from=Sender,
                                  client_ops=ClientOps,
                                  transaction=Transaction}) ->

    case pvc_key_was_updated(ClientOps, Key) of
        false ->
            HasRead = Transaction#transaction.pvc_hasread,
            VCaggr = Transaction#transaction.pvc_vcaggr,
            ok = perform_remote_read(Key, HasRead, VCaggr),
            {next_state, read_result, State};
        Value ->
            gen_fsm:reply(Sender, {ok, Value}),
            {next_state, client_command, State}
    end.

%% TODO(borja): Clean up this mess
perform_remote_read(Key, HasRead, VCaggr) ->
    {_, Node}=IndexNode = log_utilities:get_key_partition(Key),
    case Node =:= node() of
        %% Key is local
        true ->
            {_Took, ok} = timer:tc(clocksi_readitem_server, pvc_async_read, [IndexNode, Key, HasRead, VCaggr, fsm]),
            %% ok = clocksi_readitem_server:pvc_async_read(Key, HasRead, VCaggr),
            ok;
        %% Key is remote
        false ->
            ok = pvc_remote_reader:async_remote_read(IndexNode, Key, HasRead, VCaggr)
    end.

read_result({error, maxvc_bad_vc}, State) ->
    abort(State#state{abort_reason=pvc_bad_vc});

read_result({pvc_readreturn, From, {InfoMap, _Key}, Value, VCdep, VCaggr}, State=#state{transaction=Tx}) ->
    Rcv = os:timestamp(),
    InfoMap1 = maps:update_with(fsm_diff, fun(T) -> timer:now_diff(Rcv, T) end, InfoMap),
    gen_fsm:reply(State#state.from, {ok, Value, maps:merge(InfoMap1, State#state.tmp_read_map_stamps)}),
    {next_state, client_command, State#state{tmp_read_map_stamps=#{},
                                             transaction=pvc_update_transaction(From, VCdep, VCaggr, Tx)}}.

%% @doc Loop through all the keys, calling the appropriate partitions
read_batch_internal([Key | Rest]=Keys, State = #state{client_ops=ClientOps,
                                                      transaction=Transaction}) ->

    ok = perform_read_batch(Key, Transaction, ClientOps),
    {next_state, read_batch_result, State#state{read_accumulator=Keys,
                                                read_remaining=Rest}}.

-spec perform_read_batch(key(), tx(), list()) -> ok.
perform_read_batch(Key, Transaction, ClientOps) ->
    %% If the key has already been updated in this transaction,
    %% return the last assigned value directly.
    %% This works because we restrict ourselves to lww-registers,
    %% so we don't need to depend on previous values.
    case pvc_key_was_updated(ClientOps, Key) of
        false ->
            %% If the key has never been updated, request the most
            %% recent compatible version of the key to the holding
            %% partition.
            %%
            %% We will wait for the reply on the next state.
            HasRead = Transaction#transaction.pvc_hasread,
            VCaggr = Transaction#transaction.pvc_vcaggr,
            clocksi_readitem_server:pvc_async_read(Key, HasRead, VCaggr);
        Value ->
            %% If updated, reply to ourselves with the last value.
            gen_fsm:send_event(self(), {pvc_key_was_updated, Key, Value})
    end.

%% @doc Check if a key was updated by the client.
%%
%%      If it was, return the assigned value, returns false otherwise
%%
%%      Note that this function assumes that only lww-registers are being used,
%%      and that the client operations only contain one entry per key (we discard
%%      the rest when a new one is issued, see pvc_perform_update/3.
%%
-spec pvc_key_was_updated(list(), key()) -> op_param() | false.
pvc_key_was_updated(ClientOps, Key) ->
    case lists:keyfind(Key, 1, ClientOps) of
        {Key, Value} ->
            Value;

        false ->
            false
    end.

update_internal(Key, Value, State=#state{from=Sender,
                                         client_ops = ClientOps,
                                         updated_partitions = UpdatedPartitions}) ->
    gen_fsm:reply(Sender, ok),

    {NewPartitions, NewClientOps} = update_state(Key, Value, ClientOps, UpdatedPartitions),
    {next_state, client_command, State#state{client_ops=NewClientOps, updated_partitions=NewPartitions}}.

update_batch_internal(Updates, State=#state{from=Sender}) ->
    gen_fsm:reply(Sender, ok),

    NewState = lists:foldl(fun({Key, Value}, Acc) ->
        {NewPartitions, NewClientOps} = update_state(Key,
                                                     Value,
                                                     Acc#state.client_ops,
                                                     Acc#state.updated_partitions),
        Acc#state{client_ops=NewClientOps, updated_partitions=NewPartitions}
    end, State, Updates),

    {next_state, client_command, NewState}.

update_state(Key, Value, ClientOps, UpdatedPartitions) ->
    NewUpdatedPartitions = pvc_swap_writeset(UpdatedPartitions, Key, Value),
    UpdatedOps = pvc_swap_operations(ClientOps, Key, Value),
    {NewUpdatedPartitions, UpdatedOps}.

pvc_swap_writeset(UpdatedPartitions, Key, Value) ->
    Update = {Key, antidote_crdt_lwwreg, {assign, Value}},
    Partition = log_utilities:get_key_partition(Key),
    case lists:keyfind(Partition, 1, UpdatedPartitions) of
        false ->
            [{Partition, [Update]} | UpdatedPartitions];
        {Partition, WS} ->
            NewWS = case lists:keyfind(Key, 1, WS) of
                        false ->
                            [Update | WS];
                        _ ->
                            lists:keyreplace(Key, 1, WS, Update)
                    end,
            lists:keyreplace(Partition, 1, UpdatedPartitions, {Partition, NewWS})
    end.

pvc_swap_operations(ClientOps, Key, Value) ->
    Update = {Key, Value},
    case lists:keyfind(Key, 1, ClientOps) of
        false ->
            [Update | ClientOps];
        _ ->
            lists:keyreplace(Key, 1, ClientOps, Update)
    end.

%% @doc After asynchronously reading a batch of keys, collect the responses here
%% Read abort
read_batch_result({error, maxvc_bad_vc}, State) ->
    read_abort(State#state{abort_reason=pvc_bad_vc});

read_batch_result({pvc_key_was_updated, Key, Value}, State) ->
    %% The key has been already updated, returns the value directly
    %% Update the state and loop until all keys have been read
    read_batch_loop(Key, Value, State);

read_batch_result({pvc_readreturn, From, Key, Value, VCdep, VCaggr}, State=#state{transaction=Transaction}) ->
    %% A message from the read_item server, must update the transaction
    %% Update Transaction state (read partitions, version vectors, etc)
    %% Update the state and loop until all keys have been read
    read_batch_loop(Key, Value, State#state{transaction=pvc_update_transaction(From, VCdep, VCaggr, Transaction)}).

-spec read_batch_loop(key(), val(), #state{}) -> _.
read_batch_loop(Key, Value, State=#state{from=Sender,
                                         client_ops=ClientOps,
                                         transaction=Transaction,
                                         read_accumulator = ReadAcc,
                                         read_remaining = RemainingKeys}) ->

    %% Replace the key with the value inside RequestedKeys
    ReadValues = replace(ReadAcc, Key, Value),
    case RemainingKeys of
        [] ->
            %% If this was the last key to be read, return the results to the client
            gen_fsm:reply(Sender, {ok, ReadValues}),
            {next_state, client_command, State#state{read_accumulator=[]}};

        [NextKey | Rest] ->
            %% If there are keys left to read, schedule next read and loop back
            ok = perform_read_batch(NextKey, Transaction, ClientOps),
            {next_state, read_batch_result, State#state{read_remaining=Rest,
                                                        read_accumulator=ReadValues}}
    end.

-spec pvc_update_transaction(partition_id(), pvc_vc(), pvc_vc(), tx()) -> tx().
pvc_update_transaction(FromPartition, VCdep, VCaggr, Transaction = #transaction{
    pvc_hasread = HasRead,
    pvc_vcdep = TVCdep,
    pvc_vcaggr = TVCaggr
}) ->

    NewHasRead = sets:add_element(FromPartition, HasRead),

    NewVCdep = pvc_vclock:max(TVCdep, VCdep),
    NewVCaggr = pvc_vclock:max(TVCaggr, VCaggr),

    Transaction#transaction{pvc_hasread=NewHasRead,
                            pvc_vcdep = NewVCdep,
                            pvc_vcaggr = NewVCaggr}.

prepare_internal(State=#state{from=From,
                              transaction=Transaction,
                              updated_partitions=UpdatedPartitions}) ->

    case UpdatedPartitions of
        [] ->
            gen_fsm:reply(From, ok),
            recycle_fsm();

        _ ->
            Ack = length(UpdatedPartitions),
            ok = clocksi_vnode:prepare(UpdatedPartitions, Transaction),
            InitialCommitVC = Transaction#transaction.pvc_vcdep,
            {next_state, pvc_receive_votes, State#state{num_to_ack=Ack, ack_outcome={ok, InitialCommitVC}}}
    end.



pvc_receive_votes({vote, From, Vote}, State=#state{num_to_ack=NumToAck,
                                                                     ack_outcome=AccOutcome}) ->

    case Vote of
        {error, Reason} ->
            pvc_decide(State#state{num_to_ack=0, ack_outcome={error, Reason}});

        {ok, SeqNumber} ->
            {ok, PrevCommitVC} = AccOutcome,
            %% Update the commit vc with the sequence number from the partition.
            CommitVC = pvc_vclock:set_time(From, SeqNumber, PrevCommitVC),
            NewState = State#state{ack_outcome={ok, CommitVC}},
            case NumToAck > 1 of
                true ->
                    {next_state, pvc_receive_votes, NewState#state{num_to_ack=NumToAck - 1}};
                false ->
                    pvc_decide(NewState)
            end
    end.

pvc_decide(#state{from=From,
                  ack_outcome=Outcome,
                  transaction=#transaction{txn_id=TxId},
                  updated_partitions=UpdatedPartitions,
                  index_buffer=ToIndexDict}) ->

    Reply = case Outcome of
        {ok, _} -> ok;
        Err -> Err
    end,
    gen_fsm:reply(From, Reply),

    %% Merge the partition updates and the partition keyset
    OpsAndIndices = lists:map(fun({Part, WS}) ->
        case dict:find(Part, ToIndexDict) of
            error ->
                {Part, WS, []};
            {ok, Set} ->
                {Part, WS, ordsets:to_list(Set)}
        end
    end, UpdatedPartitions),

    ok = clocksi_vnode:pvc_decide(OpsAndIndices, TxId, Outcome),
    recycle_fsm().

read_abort(#state{from=From, abort_reason=Reason}) ->
    gen_fsm:reply(From, {error, Reason}),
    recycle_fsm().

abort(CoordState = #state{from=From,
                          transaction=Transaction,
                          abort_reason=AbortReason,
                          updated_partitions=UpdatedPartitions}) ->

    ok = clocksi_vnode:abort(UpdatedPartitions, Transaction),
    gen_fsm:reply(From, {error, AbortReason}),
    {stop, normal, CoordState}.

%% gen_fsm default callbacks
handle_info(_Info, _StateName, StateData) -> {stop, badmsg, StateData}.
handle_event(_Event, _StateName, StateData) -> {stop, badmsg, StateData}.
handle_sync_event(stop, _From, _StateName, StateData) -> {stop, normal, ok, StateData};
handle_sync_event(_Event, _From, _StateName, StateData) -> {stop, badmsg, StateData}.
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.
terminate(_Reason, _SN, _SD) -> ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

recycle_fsm() ->
    {next_state, wait_checkin, fresh_state(fresh_transaction())}.

fresh_transaction() ->
    TransactionId = #tx_id{server_pid=self()},
    #transaction{txn_id=TransactionId,
                 pvc_vcaggr=pvc_vclock:new(),
                 pvc_vcdep=pvc_vclock:new(),
                 pvc_hasread=sets:new(),
                 transactional_protocol=pvc}.

fresh_state(Transaction) ->
    #state{from = not_ready,
           transaction = Transaction,
           read_accumulator = [],
           read_remaining = [],
           updated_partitions = [],
           client_ops = [],
           abort_reason = undefined,
           num_to_ack = 0,
           ack_outcome = undefined,
           index_buffer = dict:new(),
           tmp_read_map_stamps = #{}}.

%% @doc Replaces the first occurrence of a key
%%
%%      Errors if the key is not present in the list
%%
-spec replace([key() | val()], key(), val()) -> [key() | val()] | error.
replace([], _, _) ->
    error;

replace([Key | Rest], Key, NewKey) ->
    [NewKey | Rest];

replace([NotMyKey | Rest], Key, NewKey) ->
    [NotMyKey | replace(Rest, Key, NewKey)].
