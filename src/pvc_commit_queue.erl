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

-module(pvc_commit_queue).

-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type writeset() :: #{key() => val()}.
-opaque cqueue() :: queue:queue(txid()).
-export_type([cqueue/0, writeset/0]).

-export([new/0,
         enqueue/2,
         dequeue_ready/3]).

-spec new() -> cqueue().
new() -> queue:new().

-spec enqueue(txid(), cqueue()) -> cqueue().
enqueue(TxId, Queue) ->
    queue:in(TxId, Queue).

-spec dequeue_ready(cqueue(), cache_id(), cache_id()) -> {[{txid(), writeset(), pvc_vc()}], cqueue()}.
dequeue_ready(Queue, DecideTable, WritesetTable) ->
    {Acc, NewCQueue} = get_ready(queue:out(Queue), DecideTable, WritesetTable, []),
    {lists:reverse(Acc), NewCQueue}.

get_ready({empty, Queue}, _DecideTable, _WritesetTable, Acc) ->
    {Acc, Queue};

get_ready({{value, TxId}, Queue}, DecideTable, WritesetTable, Acc) ->
    case ets:take(DecideTable, TxId) of
        [{TxId, abort}] ->
            get_ready(queue:out(Queue), DecideTable, WritesetTable, Acc);
        [{TxId, ready, VC}] ->
            [{TxId, WriteSet}] = ets:take(WritesetTable, TxId),
            NewAcc = [{TxId, WriteSet, VC} | Acc],
            get_ready(queue:out(Queue), DecideTable, WritesetTable, NewAcc);
        [] ->
            %% Queue head is still pending, put it back in
            {Acc, queue:in_r(TxId, Queue)}
    end.

-ifdef(TEST).

pvc_commit_queue_ready_same_test() ->
    Decide = ets:new(decide_table, [set]),
    WriteSets = ets:new(writeset_table, [set]),

    CQ = pvc_commit_queue:new(),

    %% If there are no ready elements nor deleted, the queue stays the same
    {Elts, CQ1} = pvc_commit_queue:dequeue_ready(CQ, Decide, WriteSets),
    ?assertEqual([], Elts),
    ?assertEqual(CQ, CQ1),

    CQ2 = pvc_commit_queue:enqueue(id, CQ1),
    {Elts1, CQ3} = pvc_commit_queue:dequeue_ready(CQ2, Decide, WriteSets),
    %% Dequeue is idempotent, calling it again will not modify the queue
    {_, CQ4} = pvc_commit_queue:dequeue_ready(CQ2, Decide, WriteSets),

    %% If there are no ready elements nor deleted, the queue stays the same
    ?assertEqual([], Elts1),
    ?assertEqual(CQ3, CQ4),

    ets:delete(Decide),
    ets:delete(WriteSets).

pvc_commit_queue_ready_skip_test() ->
    Decide = ets:new(decide_table, [set]),
    WriteSets = ets:new(writeset_table, [set]),

    CQ = pvc_commit_queue:new(),
    CQ1 = pvc_commit_queue:enqueue(id, CQ),
    CQ2 = pvc_commit_queue:enqueue(id1, CQ1),
    CQ3 = pvc_commit_queue:enqueue(id2, CQ2),

    %% This would happen atomically during the prepare phase
    true = ets:insert(WriteSets, [{id, #{}},
                                  {id1, #{}},
                                  {id2, #{}}]),

    %% Mark id1 as aborted, id2 as ready, id is pending (we remove id1's writeset)
    true = ets:delete(WriteSets, id1),
    true = ets:insert(Decide, [{id1, abort},
                               {id2, ready, []}]),

    %% id is still pending, so it will block other transactions in the queue
    {Elts, CQ4} = pvc_commit_queue:dequeue_ready(CQ3, Decide, WriteSets),
    ?assertEqual([], Elts),

    %% Marking id as ready should unblock the queue
    true = ets:insert(Decide, {id, ready, []}),

    %% Get ready skips removed entries from the queue
    {Elts1, CQ5} = pvc_commit_queue:dequeue_ready(CQ4, Decide, WriteSets),
    %% The entries are in the same order as we put them into the queue
    ?assertMatch([{id, #{}, []}, {id2, #{}, []}], Elts1),

    %% the queue should be empty now
    Empty = pvc_commit_queue:new(),
    ?assertEqual(Empty, CQ5),

    ets:delete(Decide),
    ets:delete(WriteSets).

-endif.
