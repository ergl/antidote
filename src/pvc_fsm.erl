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

-include("antidote.hrl").

%% supervisor callback
-export([start_link/1]).

%% states
-export([client_command/3,
         read_result/2,
         decide_vote/2]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-type ws() :: orddict:orddict(key(), val()).
-type partitions() :: ordsets:ordset(index_node()).

-export_type([ws/0,
              partitions/0]).

%% TODO(borja): Implement index operations
%% TODO(borja): Make a per-partition writeset to send to pvc_storage_vnode
%%              Right now, it's global. That's a problem for unsafe_load
-record(state, {
    %% Client PID
    from :: pid(),
    %% Active Transaction Object
    transaction :: tx(),
    %% Mapping of keys -> values
    writeset = orddict:new() :: ws(),
    %% Set of updated partitions
    updated_partitions = ordsets:new() :: partitions(),
    %% Reason to abort
    abort_reason = undefined :: atom(),
    %% Partitions to Ack
    num_to_ack = 0 :: non_neg_integer(),
    %% Commit status
    ack_outcome = undefined :: undefined | {ok, pvc_vc()} | {error, reason()}
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
    Transaction = create_transaction(),
    From ! {ok, Transaction#transaction.txn_id},
    {ok, client_command, #state{from=From,
                                transaction=Transaction}}.

%% States

-spec client_command(term(), pid(), fsm_state()) -> step(_) | stop().
client_command({unsafe_load, NKeys, Size}, Sender, State) ->
    unsafe_load_internal(NKeys, Size, State#state{from=Sender});

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

-spec create_transaction() -> tx().
create_transaction() ->
    _ = rand_compat:seed(erlang:phash2([node()]),
                         erlang:monotonic_time(),
                         erlang:unique_integer()),

    Now = dc_utilities:now_microsec(),
    CompatTime = vectorclock:new(),

    TxId = #tx_id{server_pid=self(),
                  local_start_time=Now},


    #transaction{txn_id=TxId,
                 snapshot_time=CompatTime,
                 vec_snapshot_time=CompatTime,
                 pvc_hasread=sets:new(),
                 pvc_vcdep=pvc_vclock:new(),
                 pvc_vcaggr=pvc_vclock:new()}.

-spec update_transaction(partition_id(), pvc_vc(), pvc_vc(), tx()) -> tx().
update_transaction(From, VCdep, Vcaggr, Tx = #transaction{
    pvc_hasread = HasRead,
    pvc_vcaggr = T_VCaggr,
    pvc_vcdep = T_VCdep
}) ->
    NewHasRead =  sets:add_element(From, HasRead),
    NewVCdep = pvc_vclock:max(T_VCdep, VCdep),
    NewVCaggr = pvc_vclock:max(T_VCaggr, Vcaggr),

    Tx#transaction{pvc_vcdep=NewVCdep,
                   pvc_vcaggr=NewVCaggr,
                   pvc_hasread=NewHasRead}.

-spec unsafe_load_internal(non_neg_integer(), non_neg_integer(), fsm_state()) -> stop().
unsafe_load_internal(NKeys, Size, State) ->
    Val = crypto:strong_rand_bytes(Size),
    NewState = unsafe_loop(NKeys, Val, State),
    commit(NewState).

unsafe_loop(0, _, State) ->
    State;

unsafe_loop(N, Val, State=#state{writeset=WS, updated_partitions=Partitions}) ->
    Key = integer_to_binary(N, 36),
    Partition = log_utilities:get_key_partition(Key),
    NewWriteSet = orddict:store(Key, Val, WS),
    NewPartitions = ordsets:add_element(Partition, Partitions),
    NewState = State#state{writeset=NewWriteSet, updated_partitions=NewPartitions},
    unsafe_loop(N - 1, Val, NewState).

-spec read_internal(key(), fsm_state()) -> step(read_result | client_command).
read_internal(Key, State=#state{from=Sender,
                                writeset=WS,
                                transaction=Transaction}) ->
    case orddict:find(Key, WS) of
        error ->
            HasRead = Transaction#transaction.pvc_hasread,
            VCaggr = Transaction#transaction.pvc_vcaggr,
            pvc_read_replica:async_read(Key, HasRead, VCaggr),
            {next_state, read_result, State};

        {ok, Value} ->
            gen_fsm:reply(Sender, {ok, Value}),
            {next_state, client_command, State}
    end.

%% TODO(borja): Implement this
-spec read_batch_internal(key(), fsm_state()) -> step(client_command).
read_batch_internal(_Keys, State) ->
    gen_fsm:reply(State#state.from, {ok, []}),
    {next_state, client_command, State}.

-spec update_internal(key(), val(), fsm_state()) -> step(client_command).
update_internal(Key, Value, State=#state{from=Sender,
                                         writeset=WS,
                                         updated_partitions=UpdatedPartitions}) ->
    gen_fsm:reply(Sender, ok),

    Partition = log_utilities:get_key_partition(Key),
    NewWriteSet = orddict:store(Key, Value, WS),
    NewPartitions = ordsets:add_element(Partition, UpdatedPartitions),
    {next_state, client_command, State#state{writeset=NewWriteSet,
                                             updated_partitions=NewPartitions}}.

-spec update_batch_internal([{key(), val()}], fsm_state()) -> step(client_command).
update_batch_internal(Updates, State=#state{from=Sender}) ->

    gen_fsm:reply(Sender, ok),

    NewState = lists:foldl(fun({Key, Value}, Acc) ->
        Partition = log_utilities:get_key_partition(Key),
        NewWriteSet = orddict:store(Key, Value, Acc#state.writeset),
        NewPartitions = ordsets:add_element(Partition, Acc#state.updated_partitions),
        Acc#state{writeset=NewWriteSet, updated_partitions=NewPartitions}
    end, State, Updates),

    {next_state, client_command, NewState}.

-spec commit(fsm_state()) -> step(decide_vote) | stop().
commit(State=#state{from=From,
                    writeset=WS,
                    transaction=Transaction,
                    updated_partitions=Partitions}) ->

    case Partitions of
        [] ->
            gen_fsm:reply(From, ok),
            {stop, normal, State};

        _ ->
            Ack = ordsets:size(Partitions),
            TxId = Transaction#transaction.txn_id,
            CommitVC = Transaction#transaction.pvc_vcdep,
            ok = pvc_storage_vnode:prepare(Partitions, TxId, WS, CommitVC),
            {next_state, decide_vote, State#state{num_to_ack=Ack, ack_outcome={ok, CommitVC}}}
    end.

-spec decide(fsm_state()) -> stop().
decide(State=#state{from=Sender,
                    ack_outcome=Outcome,
                    transaction = Transaction,
                    updated_partitions = UpdatedPartitions}) ->

    OutcomeMsg = case Outcome of
        {ok, _} -> ok;
        Err -> Err
    end,
    gen_fsm:reply(Sender, OutcomeMsg),

    TxId = Transaction#transaction.txn_id,
    ok = pvc_storage_vnode:decide(UpdatedPartitions, TxId, Outcome),
    {stop, normal, State}.

-spec read_abort(fsm_state()) -> stop().
read_abort(State=#state{from=From,
                        abort_reason=Reason}) ->

    gen_fsm:reply(From, {error, Reason}),
    {stop, normal, State}.

%% gen_fsm mock callbacks
code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.
terminate(_Reason, _SN, _SD) -> ok.
handle_info(_Info, _StateName, StateData) -> {stop, badmsg, StateData}.
handle_event(_Event, _StateName, StateData) -> {stop, badmsg, StateData}.
handle_sync_event(stop, _From, _StateName, StateData) -> {stop, normal, ok, StateData};
handle_sync_event(_Event, _From, _StateName, StateData) -> {stop, badmsg, StateData}.
