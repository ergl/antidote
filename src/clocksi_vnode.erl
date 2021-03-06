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
-module(clocksi_vnode).
-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include("debug_log.hrl").
-include("pvc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% riak core callbacks
-export([start_vnode/1,
         init/1,
         handle_command/3,
         handle_coverage/4,
         handle_exit/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_command/3,
         handle_handoff_data/2,
         encode_handoff_item/2,
         is_empty/1,
         terminate/2,
         delete/1]).

%% clocksi exports
-export([read_data_item/5,
         async_read_data_item/4,
         prepare/2,
         abort/2,
         commit/3,
         send_min_prepared/1,
         get_active_txns_key/3,
         get_active_txns/2,
         single_commit/2,
         single_commit_sync/2,
         reverse_and_filter_updates_per_key/2]).

%% health check export
-export([check_tables_ready/0,
         check_servers_ready/0]).

%% util exports
-export([get_cache_name/2,
         now_microsec/1]).

%% start_vnode/1 is called internally by riak_core
%%
%% check_*_ready/0 are called by external script (rpc)
%% to join a cluster together
%%
-ignore_xref([start_vnode/1,
              check_servers_ready/0,
              check_tables_ready/0]).

-record(state, {
    %% the partition that the vnode is responsible for.
    partition :: partition_id(),
    %% the prepared txn for each key. Note that for each key,
    %% there can be at most one prepared txn in any time
    %% key -> tx_id
    prepared_tx :: cache_id(),
    %% key -> tx_id
    %% the transaction id of the last committed
    %% transaction for each key.
    committed_tx :: cache_id(),
    read_servers :: non_neg_integer(),
    prepared_dict :: orddict:orddict()
}).

%%%===================================================================
%%% API
%%%===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Sends a read request to the Node that is responsible for the Key
%%      this does not actually touch the vnode, instead reads directly
%%      from the ets table to allow for concurrency
read_data_item(Node, TxId, Key, Type, Updates) ->
    case clocksi_readitem_server:read_data_item(Node, Key, Type, TxId) of
        {ok, Snapshot} ->
            Updates2 = reverse_and_filter_updates_per_key(Updates, Key),
            Snapshot2 = clocksi_materializer:materialize_eager(Type, Snapshot, Updates2),
            {ok, Snapshot2};
        {error, Reason} ->
            {error, Reason}
    end.

async_read_data_item(Node, Transaction, Key, Type) ->
    clocksi_readitem_server:async_read_data_item(
        Node,
        Key,
        Type,
        Transaction,
        {fsm, self()}
    ).

%% @doc Return active transactions in prepare state with their preparetime for a given key
%% should be run from same physical node
get_active_txns_key(Key, Partition, TableName) ->
    case ets:info(TableName) of
        undefined ->
            riak_core_vnode_master:sync_command(
                {Partition, node()},
                {get_active_txns, Key},
                clocksi_vnode_master,
                infinity
            );

        _ ->
            get_active_txns_key_internal(Key, TableName)
    end.

get_active_txns_key_internal(Key, TableName) ->
    ActiveTxs = case ets:lookup(TableName, Key) of
        [] ->
            [];
        [{Key, List}] ->
            List
    end,
    {ok, ActiveTxs}.

%% @doc Return active transactions in prepare state with their preparetime for all keys for this partition
%% should be run from same physical node
get_active_txns(Partition, TableName) ->
    case ets:info(TableName) of
        undefined ->
            riak_core_vnode_master:sync_command(
                {Partition, node()},
                {get_active_txns},
                clocksi_vnode_master,
                infinity
            );

        _ ->
            get_active_txns_internal(TableName)
    end.

get_active_txns_internal(TableName) ->
    ActiveTxs = case ets:tab2list(TableName) of
        [] ->
            [];

        Txns ->
            lists:foldl(fun
                ({_Key, []}, Acc) ->
                    Acc;
                ({_Key, List}, Acc) ->
                    List ++ Acc
            end, [], Txns)
    end,
    {ok, ActiveTxs}.

send_min_prepared(Partition) ->
    dc_utilities:call_local_vnode(Partition, clocksi_vnode_master, {send_min_prepared}).

%% @doc Sends a prepare request to a Node involved in a tx identified by TxId
prepare(ListofNodes, TxId) ->
    lists:foldl(fun({Node, WriteSet}, _Acc) ->
        riak_core_vnode_master:command(
            Node,
            {prepare, TxId, WriteSet},
            {fsm, undefined, self()},
            ?CLOCKSI_MASTER
        )
    end, ok, ListofNodes).

%% @doc Sends prepare+commit to a single partition
%%      Called by a Tx coordinator when the tx only
%%      affects one partition
single_commit([{Node, WriteSet}], TxId) ->
    riak_core_vnode_master:command(
        Node,
        {single_commit, TxId, WriteSet},
        {fsm, undefined, self()},
        ?CLOCKSI_MASTER
    ).

single_commit_sync([{Node, WriteSet}], TxId) ->
    riak_core_vnode_master:sync_command(
        Node,
        {single_commit, TxId, WriteSet},
        ?CLOCKSI_MASTER
    ).

%% @doc Sends a commit request to a Node involved in a tx identified by TxId
commit(ListofNodes, TxId, CommitTime) ->
    lists:foldl(fun({Node, WriteSet}, _Acc) ->
        riak_core_vnode_master:command(
            Node,
            {commit, TxId, CommitTime, WriteSet},
            {fsm, undefined, self()},
            ?CLOCKSI_MASTER
        )
    end, ok, ListofNodes).

%% @doc Sends an abort request to a Node involved in a tx identified by TxId
abort(ListofNodes, TxId) ->
    lists:foldl(fun({Node, WriteSet}, _Acc) ->
        riak_core_vnode_master:command(
            Node,
            {abort, TxId, WriteSet},
            {fsm, undefined, self()},
            ?CLOCKSI_MASTER
        )
    end, ok, ListofNodes).

get_cache_name(Partition, Base) ->
    BinNode = atom_to_binary(node(), latin1),
    BinBase = atom_to_binary(Base, latin1),
    BinPart = integer_to_binary(Partition),
    Name = <<BinNode/binary, BinBase/binary, <<"-">>/binary, BinPart/binary>>,
    case catch binary_to_existing_atom(Name, latin1) of
        {'EXIT', _} -> binary_to_atom(Name, latin1);
        Normal -> Normal
    end.

%% @doc Initializes all data structures that vnode needs to track information
%%      the transactions it participates on.
init([Partition]) ->
    PreparedTx = open_table(Partition, prepared),
    CommittedTx = open_table(Partition, committed_tx),

    {ok, #state{
        partition = Partition,
        prepared_tx = PreparedTx,
        committed_tx = CommittedTx,
        read_servers = ?READ_CONCURRENCY,
        prepared_dict = orddict:new()}}.

%% @doc The table holding the prepared transactions is shared with concurrent
%%      readers, so they can safely check if a key they are reading is being updated.
%%      This function checks whether or not all tables have been intialized or not yet.
%%      Returns true if the have, false otherwise.
check_tables_ready() ->
    PartitionList = dc_utilities:get_all_partitions_nodes(),
    check_table_ready(PartitionList).

check_table_ready([]) ->
    true;

check_table_ready([{Partition, Node} | Rest]) ->
    Result =
    try
        riak_core_vnode_master:sync_command({Partition, Node},
                        {check_tables_ready},
                        ?CLOCKSI_MASTER,
                        infinity)
    catch
        _:_Reason ->
        false
    end,
    case Result of
        true ->
            check_table_ready(Rest);
        false ->
            false
    end.

check_servers_ready() ->
    check_servers_ready(dc_utilities:get_all_partitions_nodes()).

check_servers_ready([]) ->
    true;

check_servers_ready([IndexNode | Rest]) ->
    Ready = try
        riak_core_vnode_master:sync_command(IndexNode, check_servers_ready, ?CLOCKSI_MASTER, infinity)
    catch _:_Reason ->
        false
    end,
    case Ready of
        true -> check_servers_ready(Rest);
        false -> false
    end.
-spec open_table(partition_id(), atom()) -> atom() | ets:tid().
open_table(Partition, Name) ->
    open_table(Partition, Name, [set, protected, named_table, ?TABLE_CONCURRENCY]).

open_table(Partition, Name, Options) ->
    CacheName = get_cache_name(Partition, Name),
    case ets:info(CacheName) of
        undefined ->
            ets:new(CacheName, Options);
        _ ->
            %% Other vnode hasn't finished closing tables
            lager:debug("Unable to open ets table in clocksi vnode, retrying"),
            timer:sleep(100),
            try
                ets:delete(CacheName)
            catch
                _:_Reason ->
                    ok
            end,
            open_table(Partition, Name, Options)
    end.

handle_command({hello}, _Sender, State) ->
  {reply, ok, State};

handle_command({check_tables_ready}, _Sender, SD0 = #state{partition = Partition}) ->
    Result = case ets:info(get_cache_name(Partition, prepared)) of
                 undefined ->
                     false;
                 _ ->
                     true
             end,
    {reply, Result, SD0};

handle_command({send_min_prepared}, _Sender,
           State = #state{partition = Partition, prepared_dict = PreparedDict}) ->
    {ok, Time} = get_min_prep(PreparedDict),
    dc_utilities:call_local_vnode(Partition, logging_vnode_master, {send_min_prepared, Time}),
    {noreply, State};

%% @doc Called by inter_dc_manager to start the read servers
handle_command(start_read_servers, _From, SD0=#state{partition=Partition, read_servers=Serv}) ->
    ok = clocksi_readitem_server:start_read_servers(Partition, Serv),
    Result = clocksi_readitem_server:check_partition_ready(node(), Partition, Serv),
    {reply, Result, SD0};

%% @doc Check read servers health
handle_command(check_servers_ready, _Sender, SD0 = #state{partition=Partition, read_servers=Serv}) ->
    Result = clocksi_readitem_server:check_partition_ready(node(), Partition, Serv),
    {reply, Result, SD0};

handle_command({prepare, Transaction, WriteSet}, _Sender, State) ->
    do_prepare(prepare_commit, Transaction, WriteSet, State);

%% @doc This is the only partition being updated by a transaction,
%%      thus this function performs both the prepare and commit for the
%%      coordinator that sent the request.
handle_command({single_commit, Transaction, WriteSet}, _Sender, State) ->
    do_prepare(single_commit, Transaction, WriteSet, State);

%% TODO: sending empty writeset to clocksi_downstream_generatro
%% Just a workaround, need to delete downstream_generator_vnode
%% eventually.
handle_command({commit, Transaction, TxCommitTime, Updates}, _Sender, State) ->
    Result = commit(Transaction, TxCommitTime, Updates, State),
    case Result of
        {ok, committed, NewPreparedDict} ->
            {reply, committed, State#state{prepared_dict = NewPreparedDict}};

        {error, no_updates} ->
            {reply, no_tx_record, State};

        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_command({abort, Transaction, Updates}, _Sender, State) ->
    TxId = Transaction#transaction.txn_id,
    case Updates of
        [{Key, _Type,  _Update} | _Rest] ->
            LogId = log_utilities:get_logid_from_key(Key),
            Node = log_utilities:get_key_partition(Key),
            LogRecord = #log_operation{tx_id = TxId, op_type = abort, log_payload = #abort_log_payload{}},
            _Result = logging_vnode:append(Node, LogId, LogRecord),
            NewPreparedDict = clean_and_notify(TxId, Updates, State),
            {reply, ack_abort, State#state{prepared_dict = NewPreparedDict}};

        _ ->
            {reply, {error, no_tx_record}, State}
    end;

handle_command({get_active_txns}, _Sender, State = #state{partition = Partition}) ->
    {reply, get_active_txns_internal(Partition), State};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(StatName, Val) ->
    term_to_binary({StatName, Val}).

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{partition=Partition, read_servers=Serv}) ->
    try
        ets:delete(get_cache_name(Partition, prepared))
    catch
        _:Reason ->
            lager:error("Error closing table ~p", [Reason])
    end,
    ok = clocksi_readitem_server:stop_read_servers(Partition, Serv),
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

do_prepare(SingleCommit, Transaction, WriteSet, State = #state{
    prepared_dict = PreparedTimes,
    prepared_tx = PreparedTransactions,
    committed_tx = CommittedTransactions
}) ->
    PrepareTime = dc_utilities:now_microsec(),
    {Result, NewPrepareTime, NewPreparedTimes} = prepare(
        Transaction,
        WriteSet,
        CommittedTransactions,
        PreparedTransactions,
        PrepareTime,
        PreparedTimes
    ),

    NewState = State#state{prepared_dict = NewPreparedTimes},
    case Result of
        {error, timeout} ->
            {reply, {error, timeout}, NewState};

        {error, no_updates} ->
            {reply, {error, no_tx_record}, NewState};

        {error, write_conflict} ->
            {reply, abort, State};

        {ok, _} ->
            case SingleCommit of
                prepare_commit ->
                    {reply, {prepared, NewPrepareTime}, NewState};

                single_commit ->
                    ResultCommit = commit(
                        Transaction,
                        NewPrepareTime,
                        WriteSet,
                        NewState
                    ),

                    case ResultCommit of
                        {ok, committed, FinalPreparedTimes} ->
                            {reply, {committed, NewPrepareTime}, NewState#state{prepared_dict = FinalPreparedTimes}};

                        {error, no_updates} ->
                            {reply, no_tx_record, NewState};

                        {error, Reason} ->
                            {reply, {error, Reason}, NewState}
                    end
            end
    end.

prepare(Transaction, TxWriteSet, CommittedTx, PreparedTx, PrepareTime, PreparedDict) ->
    TxId = Transaction#transaction.txn_id,
    case certification_check(TxId, TxWriteSet, CommittedTx, PreparedTx) of
        true ->
            case TxWriteSet of
                [{Key, _Type, _Update} | _] ->
                    Dict = set_prepared(PreparedTx, TxWriteSet, TxId, PrepareTime),
                    NewPrepare = dc_utilities:now_microsec(),
                    ok = reset_prepared(PreparedTx, TxWriteSet, TxId, NewPrepare, Dict),
                    NewPreparedDict = orddict:store(NewPrepare, TxId, PreparedDict),
                    LogRecord = #log_operation{
                        tx_id = TxId,
                        op_type = prepare,
                        log_payload = #prepare_log_payload{
                            prepare_time = NewPrepare
                        }
                    },
                    LogId = log_utilities:get_logid_from_key(Key),
                    Node = log_utilities:get_key_partition(Key),
                    Result = logging_vnode:append(Node, LogId, LogRecord),
                    {Result, NewPrepare, NewPreparedDict};

                _ ->
                    {{error, no_updates}, 0, PreparedDict}
            end;

        false ->
            {{error, write_conflict}, 0, PreparedDict}
    end.

set_prepared(PreparedTx, TxWriteSet, TxId, PrepareTime) ->
    set_prepared(PreparedTx, TxWriteSet, TxId, PrepareTime, dict:new()).

set_prepared(_PreparedTx, [], _TxId, _Time, Acc) ->
    Acc;

set_prepared(PreparedTx, [{Key, _Type, _Update} | Rest], TxId, Time, Acc) ->
    ActiveTxs = case ets:lookup(PreparedTx, Key) of
                    [] ->
                        [];
                    [{Key, List}] ->
                        List
                end,
    case lists:keymember(TxId, 1, ActiveTxs) of
        true ->
            set_prepared(PreparedTx, Rest, TxId, Time, Acc);
        false ->
            true = ets:insert(PreparedTx, {Key, [{TxId, Time} | ActiveTxs]}),
            set_prepared(PreparedTx, Rest, TxId, Time, dict:append_list(Key, ActiveTxs, Acc))
    end.

reset_prepared(_PreparedTx, [], _TxId, _Time, _ActiveTxs) ->
    ok;
reset_prepared(PreparedTx, [{Key, _Type, _Update} | Rest], TxId, Time, ActiveTxs) ->
    %% Could do this more efficiently in case of multiple updates to the same key
    true = ets:insert(PreparedTx, {Key, [{TxId, Time} | dict:fetch(Key, ActiveTxs)]}),
    lager:debug("Inserted preparing txn to PreparedTxns list ~p, [{Key, TxId, Time}]"),
    reset_prepared(PreparedTx, Rest, TxId, Time, ActiveTxs).

commit(Transaction, TxCommitTime, Updates, State = #state{
    committed_tx = CommittedTx
}) ->

    TxId = Transaction#transaction.txn_id,
    DcId = dc_meta_data_utilities:get_my_dc_id(),
    LogRecord = #log_operation{
        tx_id = TxId,
        op_type = commit,
        log_payload = #commit_log_payload{
            commit_time = {DcId, TxCommitTime},
            snapshot_time = Transaction#transaction.vec_snapshot_time
        }
    },

    case Updates of
        [{Key, _Type, _Update} | _Rest] ->
            case application:get_env(antidote, txn_cert) of
                {ok, true} ->
                    lists:foreach(fun({K, _, _}) ->
                        true = ets:insert(CommittedTx, {K, TxCommitTime})
                    end, Updates);
                _ ->
                    ok
            end,

            LogId = log_utilities:get_logid_from_key(Key),
            Node = log_utilities:get_key_partition(Key),
            case logging_vnode:append_commit(Node, LogId, LogRecord) of
                {error, timeout} ->
                    {error, timeout};

                {ok, _} ->
                    case update_materializer(Updates, Transaction, TxCommitTime) of
                        error ->
                            {error, materializer_failure};

                        ok ->
                            NewPreparedDict = clean_and_notify(TxId, Updates, State),
                            {ok, committed, NewPreparedDict}
                    end
            end;

        _ ->
            {error, no_updates}
    end.

%% @doc clean_and_notify:
%%      This function is used for cleanning the state a transaction
%%      stores in the vnode while it is being procesed. Once a
%%      transaction commits or aborts, it is necessary to clean the
%%      prepared record of a transaction T. There are three possibility
%%      when trying to clean a record:
%%      1. The record is prepared by T (with T's TxId).
%%          If T is being committed, this is the normal. If T is being
%%          aborted, it means T successfully prepared here, but got
%%          aborted somewhere else.
%%          In both cases, we should remove the record.
%%      2. The record is empty.
%%          This can only happen when T is being aborted. What can only
%%          only happen is as follows: when T tried to prepare, someone
%%          else has already prepared, which caused T to abort. Then
%%          before the partition receives the abort message of T, the
%%          prepared transaction gets processed and the prepared record
%%          is removed.
%%          In this case, we don't need to do anything.
%%      3. The record is prepared by another transaction M.
%%          This can only happen when T is being aborted. We can not
%%          remove M's prepare record, so we should not do anything
%%          either.
clean_and_notify(TxId, Updates, #state{
    prepared_tx = PreparedTx,
    prepared_dict = PreparedDict
}) ->
    ok = clean_prepared(PreparedTx, Updates, TxId),
    case get_time(PreparedDict, TxId) of
        error ->
            PreparedDict;

        {ok, Time} ->
            orddict:erase(Time, PreparedDict)
    end.

clean_prepared(_PreparedTx, [], _TxId) ->
    ok;

clean_prepared(PreparedTx, [{Key, _Type, _Update} | Rest], TxId) ->
    ActiveTxs = case ets:lookup(PreparedTx, Key) of
        [] ->
            [];
        [{Key, List}] ->
            List
    end,
    NewActive = lists:keydelete(TxId, 1, ActiveTxs),
    true = case NewActive of
        [] ->
            ets:delete(PreparedTx, Key);

        _ ->
            ets:insert(PreparedTx, {Key, NewActive})
    end,
    clean_prepared(PreparedTx, Rest, TxId).

%% @doc converts a tuple {MegaSecs, Secs, MicroSecs} into microseconds
now_microsec({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

certification_check(TxId, Updates, CommittedTx, PreparedTx) ->
    case application:get_env(antidote, txn_cert) of
        {ok, true} ->
        %io:format("AAAAH"),
        certification_with_check(TxId, Updates, CommittedTx, PreparedTx);
        _  -> true
    end.

%% @doc Performs a certification check when a transaction wants to move
%%      to the prepared state.
certification_with_check(_, [], _, _) ->
    true;
certification_with_check(TxId, [H | T], CommittedTx, PreparedTx) ->
    TxLocalStartTime = TxId#tx_id.local_start_time,
    {Key, _, _} = H,
    case ets:lookup(CommittedTx, Key) of
        [{Key, CommitTime}] ->
            case CommitTime > TxLocalStartTime of
                true ->
                    false;
                false ->
                    case check_prepared(TxId, PreparedTx, Key) of
                        true ->
                            certification_with_check(TxId, T, CommittedTx, PreparedTx);
                        false ->
                            false
                    end
            end;
        [] ->
            case check_prepared(TxId, PreparedTx, Key) of
                true ->
                    certification_with_check(TxId, T, CommittedTx, PreparedTx);
                false ->
                    false
            end
    end.

check_prepared(_TxId, PreparedTx, Key) ->
    case ets:lookup(PreparedTx, Key) of
        [] ->
            true;
        _ ->
            false
    end.

-spec update_materializer(
    DownstreamOps :: [{key(), type(), op()}],
    Transaction :: tx(),
    TxCommitTime :: non_neg_integer()
) -> ok | error.

update_materializer(DownstreamOps, Transaction, TxCommitTime) ->
    DcId = dc_meta_data_utilities:get_my_dc_id(),
    ReversedDownstreamOps = lists:reverse(DownstreamOps),
    UpdateFunction = fun({Key, Type, Op}, AccIn) ->
        CommittedDownstreamOp = #clocksi_payload{
            key = Key,
            type = Type,
            op_param = Op,
            snapshot_time = Transaction#transaction.vec_snapshot_time,
            commit_time = {DcId, TxCommitTime},
            txid = Transaction#transaction.txn_id
        },
        Res = materializer_vnode:update(Key, CommittedDownstreamOp),
        [Res| AccIn]
    end,
    Results = lists:foldl(UpdateFunction, [], ReversedDownstreamOps),
    Failed = lists:any(fun(ok) -> false; (_) -> true end, Results),
    case Failed of
        true ->
            error;
        false ->
            ok
    end.

%% Internal functions
reverse_and_filter_updates_per_key(Updates, Key) ->
    lists:foldl(fun({KeyPrime, _Type, Op}, Acc) ->
                    case KeyPrime == Key of
                        true ->
                            [Op | Acc];
                        false ->
                            Acc
                    end
                end, [], Updates).


-spec get_min_prep(list()) -> {ok, non_neg_integer()}.
get_min_prep(OrdDict) ->
    case OrdDict of
        [] ->
            {ok, dc_utilities:now_microsec()};
        [{Time, _TxId}|_] ->
            {ok, Time}
    end.

-spec get_time(list(), txid()) -> {ok, non_neg_integer()} | error.
get_time([], _TxIdCheck) ->
    error;
get_time([{Time, TxId} | Rest], TxIdCheck) ->
    case TxId == TxIdCheck of
        true ->
            {ok, Time};
        false ->
            get_time(Rest, TxIdCheck)
    end.

-ifdef(TEST).

%% @doc Testing filter_updates_per_key.
filter_updates_per_key_test() ->
    Op1 = {update, {{increment, 1}, actor1}},
    Op2 = {update, {{increment, 2}, actor1}},
    Op3 = {update, {{increment, 3}, actor1}},
    Op4 = {update, {{increment, 4}, actor1}},

    ClockSIOp1 = {a, crdt_pncounter, Op1},
    ClockSIOp2 = {b, crdt_pncounter, Op2},
    ClockSIOp3 = {c, crdt_pncounter, Op3},
    ClockSIOp4 = {a, crdt_pncounter, Op4},

    ?assertEqual([Op4, Op1],
        reverse_and_filter_updates_per_key([ClockSIOp1, ClockSIOp2, ClockSIOp3, ClockSIOp4], a)).

-endif.
