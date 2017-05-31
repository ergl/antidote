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
-module(basic_logging_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-ignore_xref([start_vnode/1]).

-export([start_vnode/1,
         is_sync_log/0,
         set_sync_log/1]).

%% TODO(borja): Remove, just for testing purposes
-export([get_logmap/1]).

%% Basic log operations
-export([read/2,
         async_read/3,
         append/3,
         async_append/4,
         append_commit/3,
         async_append_commit/4,
         append_all/3,
         async_append_all/4]).

%% Utility functions
-export([last_op_id/3]).

%% riak_core_vnode callbacks
-export([init/1,
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

-record(state, {
    partition :: partition_id(),
    %% flag, enable / disable logging to disk
    enable_log_to_disk :: boolean(),
    %% node -> log mapping
    logs_map :: dict:dict(),
    %% store the op id count. Goes from {LogId, Node} -> Latest count
    op_id_table :: cache_id()
}).

-record(log_op, {
    op_num :: #op_number{},
    payload :: term()
}).

-opaque log_op_num() :: #op_number{}.

-opaque log_op() :: #log_op{}.

-export_type([log_op/0,
              log_op_num/0]).

start_vnode(Index) ->
    riak_core_vnode_master:get_vnode_pid(Index, ?MODULE).

%% @doc Returns true if syncrounous logging is enabled
%%      False otherwise.
%%      Uses environment variable "sync_log" set in antidote.app.src
-spec is_sync_log() -> boolean().
is_sync_log() ->
    dc_meta_data_utilities:get_env_meta_data(sync_log, false).

%% @doc Takes as input a boolean to set whether or not items will
%%      be logged synchronously at this DC (sends a broadcast to update
%%      the environment variable "sync_log" to all nodes).
%%      If true, items will be logged synchronously
%%      If false, items will be logged asynchronously
-spec set_sync_log(boolean()) -> ok.
set_sync_log(Value) ->
    dc_meta_data_utilities:store_env_meta_data(sync_log, Value).

-spec get_logmap(index_node()) -> {ok, dict:dict()}.
get_logmap(Node) ->
    sync_command(Node, get_logmap).

%% @doc Gets the last id of operations stored in the log for the given DCID
-spec last_op_id(index_node(), log_id(), dcid()) -> {ok, non_neg_integer()}.
last_op_id(Node, LogId, DCId) ->
    sync_command(Node, {get_latest_op_id, LogId, DCId}).

%% @doc Sends a `read' synchronous command to the Logs in `Node'
-spec read(index_node(), key()) -> {ok, [term()]} | {error, reason()}.
read(Node, LogId) ->
    sync_command(Node, {read, LogId}).

%% @doc Sends a `read' asynchronous command to the Logs in `Preflist'
-spec async_read(index_node(), key(), sender()) -> ok.
async_read(Node, LogId, ReplyTo) ->
    async_command(Node, {read, LogId}, ReplyTo).

%% @doc Sends an `append' synchronous command to the Logs in `Node'
-spec append(index_node(), key(), term()) -> {ok, log_op_num()} | {error, reason()}.
append(Node, LogId, Op) ->
    sync_command(Node, {append, LogId, Op, false}).

%% @doc Sends an `append' asynchronous command to the Logs in `Node'
-spec async_append(index_node(), key(), term(), sender()) -> ok.
async_append(Node, LogId, Op, ReplyTo) ->
    async_command(Node, {append, LogId, Op, false}, ReplyTo).

%% Same as append, but, if enabled, will ensure items are written to disk.
-spec append_commit(index_node(), key(), term()) -> {ok, log_op_num()} | {error, reason()}.
append_commit(Node, LogId, Op) ->
    sync_command(Node, {append, LogId, Op, is_sync_log()}).

%% Same as async_append, but, if enabled, will ensure items are written to disk.
-spec async_append_commit(index_node(), key(), term(), sender()) -> ok.
async_append_commit(Node, LogId, Op, ReplyTo) ->
    async_command(Node, {append, LogId, Op, is_sync_log()}, ReplyTo).

-spec append_all(index_node(), key(), [term()]) -> {ok, log_op_num()} | {error, reason()}.
append_all(Node, LogId, Ops) ->
    sync_command(Node, {append_all, LogId, Ops, is_sync_log()}).

-spec async_append_all(index_node(), key(), [term()], sender()) -> ok.
async_append_all(Node, LogId, Ops, ReplyTo) ->
    async_command(Node, {append_all, LogId, Ops, is_sync_log()}, ReplyTo).

sync_command(To, Message) ->
    riak_core_vnode_master:sync_command(To, Message, basic_logging_vnode_master).

async_command(To, Message, From) ->
    riak_core_vnode_master:command(To, Message, From, basic_logging_vnode_master).

init([Partition]) ->
    PrefLists = preflists_with_partition(Partition),
    OpIdTable = ets:new(op_id_table, [set]),
    lager:debug("Opening logs for partition ~w", [Partition]),
    case open_logs(Partition, PrefLists, OpIdTable) of
        {error, Reason} ->
            lager:error("ERROR: opening logs for partition ~w, reason ~w", [Partition, Reason]),
            {error, Reason};

        {ok, LogMap} ->
            {ok, WriteToDisk} = application:get_env(antidote, enable_logging),
            {ok, #state{
                logs_map=LogMap,
                partition=Partition,
                op_id_table=OpIdTable,
                enable_log_to_disk=WriteToDisk
            }}
    end.

handle_command({hello}, _Sender, State) ->
    {reply, ok, State};

handle_command(get_logmap, _Sender, State) ->
    {reply, {ok, State#state.logs_map}, State};

handle_command({get_latest_op_id, LogId, DCId}, _Sender, State = #state{op_id_table=OpIdTable}) ->
    OpNum = get_latest_op_id(OpIdTable, LogId, DCId),
    {reply, {ok, OpNum#op_number.local}, State};

handle_command({read, LogId}, _Sender, State=#state{
    logs_map=LogMap
}) ->
    case get_log_from_map(LogMap, LogId) of
        no_log ->
            {reply, {error, no_log}, State};

        {ok, Log} ->
            %% Wait until all pending writes are written
            ok = disk_log:sync(Log),
            Ops = read_all(Log),
            {reply, {ok, Ops}, State}
    end;

handle_command({append, LogId, Op, ShouldSync}, _Sender, State=#state{
    logs_map=LogMap,
    op_id_table=OpIdTable,
    enable_log_to_disk=ShouldWrite
}) ->
    case get_log_from_map(LogMap, LogId) of
        no_log ->
            {reply, {error, no_log}, State};

        {ok, Log} ->
            case insert_log_op(Log, LogId, Op, OpIdTable, ShouldWrite) of
                {error, Reason} ->
                    {reply, {error, Reason}, State};

                {ok, OpId} ->
                    Res = case ShouldSync of
                        true ->
                            disk_log:sync(Log);
                        false ->
                            ok
                    end,
                    case Res of
                        ok ->
                            {reply, {ok, OpId}, State};

                        {error, Reason} ->
                            {reply, {error, Reason}, State}
                    end
            end
    end;

handle_command({append_all, LogId, ExternalOps, ShouldSync}, _Sender, State=#state{
    logs_map=LogMap,
    op_id_table=OpIdTable,
    enable_log_to_disk=ShouldWrite
}) ->
    case get_log_from_map(LogMap, LogId) of
        no_log ->
            {reply, {error, no_log}, State};

        {ok, Log} ->
            case insert_external_log_ops(Log, LogId, ExternalOps, OpIdTable, ShouldWrite) of
                {error, Reason} ->
                    {reply, {error, Reason}, State};

                {ok, OpId} ->
                    Res = case ShouldSync of
                        true ->
                            disk_log:sync(Log);
                        false ->
                            ok
                    end,
                    case Res of
                        ok ->
                            {reply, {ok, OpId}, State};

                        {error, Reason} ->
                            {reply, {error, Reason}, State}
                    end
            end
    end.

-spec build_log_op(log_id(), term(), cache_id()) -> log_op().
build_log_op(LogId, Op, OpIdTable) ->
    SelfId = dc_meta_data_utilities:get_my_dc_id(),
    OpNumber = faa_latest_op_id(OpIdTable, LogId, SelfId),
    #log_op{op_num = OpNumber, payload = Op}.

-spec faa_latest_op_id(cache_id(), log_id(), dcid()) -> log_op_num().
faa_latest_op_id(OpIdTable, LogId, DCId) ->
    OpNum = #op_number{local = Local, global = Global} = get_latest_op_id(OpIdTable, LogId, DCId),
    NewNum = OpNum#op_number{local = Local + 1, global = Global + 1},
    ok = store_latest_op_id(OpIdTable, LogId, DCId, NewNum),
    NewNum.

-spec faa_latest_global_op_id(cache_id(), log_id(), dcid()) -> log_op_num().
faa_latest_global_op_id(OpIdTable, LogId, DCId) ->
    OpNum = #op_number{global = Global} = get_latest_op_id(OpIdTable, LogId, DCId),
    NewNum = OpNum#op_number{global = Global + 1},
    ok = store_latest_op_id(OpIdTable, LogId, DCId, NewNum),
    NewNum.

-spec get_latest_op_id(cache_id(), log_id(), dcid()) -> log_op_num().
get_latest_op_id(OpIdTable, LogId, DCId) ->
    Key = {LogId, DCId},
    case ets:lookup(OpIdTable, Key) of
        [] ->
            #op_number{node = {node(), DCId}, global = 0, local = 0};
        [{Key, LatestOpId}] ->
            LatestOpId
    end.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_command(?FOLD_REQ{foldfun=FoldFun, acc0=Acc0}, _Sender, State=#state{
    logs_map=LogMap
}) ->
    F = fun({Key, LogRecord}, Acc) -> FoldFun(Key, LogRecord, Acc) end,
    Acc = join_logs(dict:to_list(LogMap), F, Acc0),
    {reply, Acc, State}.

handle_handoff_data(Data, State=#state{
    logs_map=LogMap,
    op_id_table=OpIdTable,
    enable_log_to_disk=ShouldWrite
}) ->
    {LogId, Operation} = binary_to_term(Data),
    case get_log_from_map(LogMap, LogId) of
        no_log ->
            {reply, {error, no_log}, State};

        {ok, Log} ->
            %% Optimistic handling; crash otherwise.
            {ok, _OpId} = insert_log_op(Log, LogId, Operation, OpIdTable, ShouldWrite),
            ok = disk_log:sync(Log),
            {reply, ok, State}
    end.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{logs_map=LogMap}) ->
    AllEmpty = lists:all(fun(K) ->
        log_is_empty(dict:fetch(K, LogMap))
    end, dict:fetch_keys(LogMap)),
    {AllEmpty, State}.

terminate(_Reason, _State) ->
    ok.

delete(State) ->
    {ok, State}.

%%====================%%
%% Internal Functions %%
%%====================%%

%% @doc Get all the preflists with the given partition as a member
%%
%%      Gather only from the current riak ring.
%%
-spec preflists_with_partition(partition()) -> [preflist()].
preflists_with_partition(Partition) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllPrefLists = riak_core_ring:all_preflists(Ring, ?N),
    lists:filter(fun(PrefL) -> preflist_member(Partition, PrefL) end, AllPrefLists).

-spec preflist_member(partition(), preflist()) -> boolean().
preflist_member(Partition, Preflist) ->
    lists:any(fun({P, _}) -> P =:= Partition end, Preflist).

open_logs(Partition, PrefList, OpIdTable) ->
    open_logs(Partition, PrefList, dict:new(), OpIdTable).

open_logs(_, [], LogMap, _OpIdTable) ->
    {ok, LogMap};

open_logs(Partition, [PrefList | Rest], LogMap, OpIdTable) ->
    LogName = generate_log_name(Partition, PrefList),
    case disk_log:open([{name, LogName}]) of
        {ok, LogName} ->
            ok = replay_old_op_ids(PrefList, LogName, OpIdTable),
            lager:debug("Opened log ~p, last op ids are ~p", [LogName, ets:tab2list(OpIdTable)]),
            open_logs(Partition, Rest, dict:store(PrefList, LogName, LogMap), OpIdTable);

        {repaired, Log, _Rec, _Bad} ->
            ok = replay_old_op_ids(PrefList, Log, OpIdTable),
            lager:debug("Repaired log ~p, last op ids are ~p", [Log, ets:tab2list(OpIdTable)]),
            open_logs(Partition, Rest, dict:store(PrefList, Log, LogMap), OpIdTable);

        {error, Reason} ->
            {error, Reason}
    end.

generate_log_name(Partition, PrefList) ->
    %% TODO(borja): Can remove the `basic` prefix once we remove the other logging layer
    LogFile = atom_to_list(basic) ++ integer_to_list(Partition),
    PartitionList = log_utilities:remove_node_from_preflist(PrefList),
    PartitionListString = string:join(lists:map(fun integer_to_list/1, PartitionList), "-"),
    LogName = LogFile ++ "--" ++ PartitionListString,
    filename:join(app_helper:get_env(riak_core, platform_data_dir), LogName).

-spec replay_old_op_ids(log_id(), disk_log:log(), cache_id()) -> ok | {error, reason()}.
replay_old_op_ids(LogId, Log, OpIdTable) ->
    SelfId = dc_meta_data_utilities:get_my_dc_id(),
    replay_old_op_ids(LogId, SelfId, Log, OpIdTable, start).

-spec replay_old_op_ids(log_id(), dcid(), disk_log:log(), cache_id(), disk_log:continuation() | start) -> ok | {error, reason()}.
replay_old_op_ids(LogId, DCId, Log, OpIdTable, Continuation) ->
    ok = disk_log:sync(Log),
    case disk_log:chunk(Log, Continuation) of
        {error, Reason} ->
            {error, Reason};

        eof ->
            ok;

        {NewContinuation, Terms} ->
            %% Find the greatest op number in the recovered, and store it
            MaxNumber = max_op_number(Terms),
            ok = store_latest_op_id(OpIdTable, LogId, DCId, MaxNumber),
            replay_old_op_ids(LogId, DCId, Log, OpIdTable, NewContinuation);

        {NewContinuation, Terms, BadBytes} ->
            case BadBytes > 0 of
                true ->
                    {error, bad_bytes};
                false ->
                    %% Find the greatest op number in the recovered, and store it
                    MaxNumber = max_op_number(Terms),
                    ok = store_latest_op_id(OpIdTable, LogId, DCId, MaxNumber),
                    replay_old_op_ids(LogId, DCId, Log, OpIdTable, NewContinuation)
            end
    end.

-spec max_op_number([term()]) -> log_op_num().
max_op_number(Terms) ->
    %% Operations are already ordered on the log
    {_Key, #log_op{op_num = OpNum}} = lists:last(Terms),
    OpNum.

-spec store_latest_op_id(cache_id(), log_id(), dcid(), log_op_num()) -> ok.
store_latest_op_id(OpIdTable, LogId, DCId, OpNum) ->
    Key = {LogId, DCId},
    case ets:lookup(OpIdTable, Key) of
        [] ->
            true = ets:insert(OpIdTable, {Key, OpNum}),
            ok;

        [{Key, #op_number{local = PrevLocal, global = PrevGlobal}}] ->
            #op_number{local = Local, global = Global} = OpNum,
            %% Only store if the count was greater
            case (Local > PrevLocal) or (Global > PrevGlobal) of
                true ->
                    true = ets:insert(OpIdTable, {Key, OpNum}),
                    ok;
                false ->
                    ok
            end
    end.

-spec get_log_from_map(dict:dict(), log_id()) -> {ok, log()} | no_log.
get_log_from_map(Map, LogId) ->
    case dict:find(LogId, Map) of
        {ok, Log} ->
            {ok, Log};
        error ->
            no_log
    end.

-spec insert_log_op(disk_log:log(), log_id(), term(), cache_id(), boolean()) -> {ok, log_op_num()} | {error, reason()}.
insert_log_op(Log, LogId, Op, OpIdTable, ShouldWrite) ->
    LogOp = build_log_op(LogId, Op, OpIdTable),
    Res = case ShouldWrite of
        true ->
            disk_log:log(Log, {LogId, LogOp});
        false ->
            ok
    end,
    case Res of
        ok ->
            {ok, LogOp#log_op.op_num};
        {error, Reason} ->
            {error, Reason}

    end.

-spec insert_external_log_ops(disk_log:log(), log_id(), [term()], cache_id(), boolean()) -> {ok, log_op_num()} | {error, reason()}.
insert_external_log_ops(Log, LogId, LogOps, OpIdTable, ShouldWrite) ->
    SelfId = dc_meta_data_utilities:get_my_dc_id(),
    %% Go through all given operations, and for each one, increment the current
    %% maximum global op id.
    {AllOps, LastOpNum} = lists:foldl(fun(LogOp, {AccOps, _}) ->
        faa_latest_global_op_id(OpIdTable, LogId, SelfId),
        {[{LogId, LogOp} | AccOps], LogOp#log_op.op_num}
    end, {[], ignore}, LogOps),

    Res = case ShouldWrite of
        true ->
            disk_log:log_terms(Log, lists:reverse(AllOps));

        false ->
            ok
    end,

    case Res of
        ok ->
            {ok, LastOpNum};

        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Read all the terms written to the given log.
-spec read_all(disk_log:log()) -> [term()].
read_all(Log) ->
    read_all(Log, start, []).

-spec read_all(disk_log:log(), start | disk_log:continuation(), [term()]) -> [term()].
read_all(_Log, eof, Ops) ->
    Ops;

read_all(Log, Cont, Ops) ->
    {Next, Terms} = case disk_log:chunk(Log, Cont) of
        {NextCont, ReadOps} ->
            {NextCont, ReadOps};

        {NextCont, ReadOps, _Bad} ->
            {NextCont, ReadOps};

        eof ->
            {eof, []}
    end,
    read_all(Log, Next, Terms ++ Ops).

%% @doc join_logs: Recursive fold of all the logs stored in the vnode
%%      Input:  Logs: A list of pairs {Preflist, Log}
%%                      F: Function to apply when floding the log (dets)
%%                      Acc: Folded data
%%      Return: Folded data of all the logs.
%%
-spec join_logs([{preflist(), disk_log:log()}], fun(), term()) -> term().
join_logs([], _F, Acc) ->
    Acc;

join_logs([{_Preflist, Log}|T], F, Acc) ->
    JointAcc = fold_log(Log, start, F, Acc),
    join_logs(T, F, JointAcc).

fold_log(Log, Continuation, F, Acc) ->
    case disk_log:chunk(Log, Continuation) of
        eof ->
            Acc;
        {Next, Ops} ->
            NewAcc = lists:foldl(F, Acc, Ops),
            fold_log(Log, Next, F, NewAcc)
    end.

-spec log_is_empty(disk_log:log()) -> boolean().
log_is_empty(Log) ->
    eof =:= disk_log:chunk(Log, start).
