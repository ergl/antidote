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

-record(cqueue, {
    %% The main commit TxId queue
    q :: queue:queue(txid()),
    %% Mapping between txids and their write sets
    write_sets :: #{txid() => writeset()},
    %% For the ready tx, put their ids with their commit VC
    %% and their index key list here
    ready_tx :: #{txid() => {pvc_vc(), list()}},
    %% A set of txids that have been discarded
    discarded_tx :: sets:set(txid())
}).

-opaque cqueue() :: #cqueue{}.

-type writeset() :: #{key() => val()}.

-export_type([cqueue/0]).

-export([new/0,
         enqueue/3,
         dequeue_ready/1,
         ready/4,
         remove/2,
         contains_disputed/2]).

%% TODO(borja): Use a different structure for this
%% See the parallel prepare brainstorming for ideas
-spec contains_disputed(writeset(), cqueue()) -> boolean().
contains_disputed(WS, #cqueue{write_sets = WriteSets}) ->
    is_ws_disputed(maps:to_list(WriteSets), WS).

-spec new() -> cqueue().
new() ->
    #cqueue{q = queue:new(),
            ready_tx = #{},
            write_sets = #{},
            discarded_tx = sets:new()}.

-spec enqueue(txid(), writeset(), cqueue()) -> cqueue().
enqueue(TxId, WS, CQueue = #cqueue{q = Queue, write_sets = WriteSets}) ->
    CQueue#cqueue{q = queue:in(TxId, Queue),
                  write_sets = WriteSets#{TxId => WS}}.

-spec ready(txid(), list(), pvc_vc(), cqueue()) -> cqueue().
ready(TxId, IndexList, VC, CQueue = #cqueue{
    q = Queue,
    ready_tx = ReadyMap
}) ->
    case queue:member(TxId, Queue) of
        false ->
            CQueue;
        true ->
            CQueue#cqueue{ready_tx = ReadyMap#{TxId => {VC, IndexList}}}
    end.

-spec remove(txid(), cqueue()) -> cqueue().
remove(TxId, CQueue = #cqueue{
    q = Queue,
    write_sets = WriteSets,
    discarded_tx = DiscardedSet
}) ->
    case queue:member(TxId, Queue) of
        false ->
            CQueue;
        true ->
            CQueue#cqueue{write_sets = maps:remove(TxId, WriteSets),
                          discarded_tx = sets:add_element(TxId, DiscardedSet)}
    end.

-spec dequeue_ready(cqueue()) -> {[{txid(), writeset(), pvc_vc(), list()}], cqueue()}.
dequeue_ready(#cqueue{q = Queue,
                      write_sets = WriteSets,
                      ready_tx = ReadyMap,
                      discarded_tx = DiscardedSet}) ->

    {Acc, NewCQueue} = get_ready(queue:out(Queue), WriteSets, ReadyMap, DiscardedSet, []),
    {lists:reverse(Acc), NewCQueue}.

get_ready({empty, Queue}, WriteSets, ReadyMap, DiscardedSet, Acc) ->
    {Acc, from(Queue, WriteSets, ReadyMap, DiscardedSet)};

get_ready({{value, TxId}, Queue}, WriteSets, ReadyMap, DiscardedSet, Acc) ->
    case sets:is_element(TxId, DiscardedSet) of
        true ->
            DeleteDiscard = sets:del_element(TxId, DiscardedSet),
            get_ready(queue:out(Queue), WriteSets, ReadyMap, DeleteDiscard, Acc);

        false ->
            case maps:is_key(TxId, ReadyMap) of
                false ->
                    {Acc, from(queue:in_r(TxId, Queue), WriteSets, ReadyMap, DiscardedSet)};

                true ->
                    %% Get WS and remove it
                    {WS, NewWriteSets} = maps:take(TxId, WriteSets),

                    %% Get VC and remove it
                    {{VC, IndexList}, NewReadyMap} = maps:take(TxId, ReadyMap),

                    %% Append entry to the Acc
                    NewAcc = [{TxId, WS, VC, IndexList} | Acc],
                    get_ready(queue:out(Queue), NewWriteSets, NewReadyMap, DiscardedSet, NewAcc)
            end
    end.

%% Internal

from(Queue, WriteSets, ReadyMap, DiscardedDict) ->
    #cqueue{q = Queue,
            write_sets = WriteSets,
            ready_tx = ReadyMap,
            discarded_tx = DiscardedDict}.

-spec is_ws_disputed([{txid(), writeset()}], writeset()) -> boolean().
is_ws_disputed([], _) ->
    false;

is_ws_disputed([{_TxId, OtherWS} | Rest], WS) ->
    case ws_intersect(OtherWS, maps:keys(WS)) of
        true ->
            true;
        false ->
            is_ws_disputed(Rest, WS)
    end.

ws_intersect(_Map1, []) -> false;
ws_intersect(Map1, [Key | Keys]) ->
    maps:is_key(Key, Map1) orelse ws_intersect(Map1, Keys).

-ifdef(TEST).

pvc_commit_queue_conflict_test() ->
    TestWS = #{key_a => ignore},
    TestWS1 = #{key_b => ignore},

    CQ = pvc_commit_queue:new(),

    %% Empty queues can't conflict
    ?assertEqual(false, pvc_commit_queue:contains_disputed(TestWS, CQ)),

    CQ1 = pvc_commit_queue:enqueue(id, TestWS, CQ),

    %% Empty WS can't conflict
    ?assertEqual(false, pvc_commit_queue:contains_disputed(#{}, CQ1)),

    %% Intersect on key_a
    ?assertEqual(true, pvc_commit_queue:contains_disputed(TestWS, CQ1)),

    %% Intersect happens even after marking as ready
    CQ2 = pvc_commit_queue:ready(id, [], ignore, CQ1),
    ?assertEqual(true, pvc_commit_queue:contains_disputed(TestWS, CQ2)),

    CQ3 = pvc_commit_queue:enqueue(id2, TestWS1, CQ2),
    %% Intersect does not take removed ids into account
    CQ4 = pvc_commit_queue:remove(id2, CQ3),
    ?assertEqual(false, pvc_commit_queue:contains_disputed(TestWS1, CQ4)).

pvc_commit_queue_ready_same_test() ->
    CQ = pvc_commit_queue:new(),

    %% If there are no ready elements nor deleted, the queue stays the same
    {Elts, CQ1} = pvc_commit_queue:dequeue_ready(CQ),
    ?assertEqual([], Elts),
    ?assertEqual(CQ, CQ1),

    CQ2 = pvc_commit_queue:enqueue(id, #{}, CQ1),
    {Elts1, CQ3} = pvc_commit_queue:dequeue_ready(CQ2),
    {_, CQ4} = pvc_commit_queue:dequeue_ready(CQ2),

    %% If there are no ready elements nor deleted, the queue stays the same
    ?assertEqual([], Elts1),
    ?assertEqual(CQ3, CQ4).

pvc_commit_queue_ready_skip_test() ->
    CQ = pvc_commit_queue:new(),
    CQ1 = pvc_commit_queue:enqueue(id, #{}, CQ),
    CQ2 = pvc_commit_queue:enqueue(id1, #{}, CQ1),
    CQ3 = pvc_commit_queue:enqueue(id2, #{}, CQ2),

    CQ4 = pvc_commit_queue:ready(id2, [], ignore, CQ3),
    CQ5 = pvc_commit_queue:remove(id1, CQ4),

    {Elts, CQ6} = pvc_commit_queue:dequeue_ready(CQ5),
    ?assertEqual([], Elts),

    CQ7 = pvc_commit_queue:ready(id, [], ignore, CQ6),

    %% Get ready skips removed entries from the queue
    {Elts1, CQ8} = pvc_commit_queue:dequeue_ready(CQ7),
    %% The entries are in the same order as we put them into the queue
    ?assertMatch([{id, #{}, ignore, []}, {id2, #{}, ignore, []}], Elts1),

    %% id1 is gone forever, queue stays the same
    CQ9 = pvc_commit_queue:remove(id1, CQ8),
    ?assertEqual(CQ9, CQ8),

    %% the queue should be empty now
    Empty = pvc_commit_queue:new(),
    ?assertEqual(Empty, CQ9).

-endif.