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

-module(pvc_commit_log).

-include("antidote.hrl").
-include("pvc.hrl").

%% When the log reaches this size, the log will be pruned
-define(GC_THRESHOLD, 4000).
%% The number of versions to keep after a GC pass
-define(KEEP_VERSIONS, 2000).

-record(clog, {
    at :: partition_id(),
    smallest :: non_neg_integer() | bottom,
    data :: gb_trees:tree(non_neg_integer(), pvc_vc())
}).

-type clog() :: #clog{}.

-export_type([clog/0]).

-export([new_at/1,
         insert/2,
         get_smaller_from_dots/3]).

-spec new_at(partition_id()) -> clog().
new_at(AtId) ->
    #clog{at=AtId, smallest=bottom, data=gb_trees:empty()}.

%% The Commit Log at the i-th partition (CLog_i) is only updated
%% when if there is a new transaction committed at i. This means
%% that the entries in CLog_i are strictly monotonic at their i-th
%% entry. We can use this to store the entries in a tree, where the
%% key is the i-th value of an entry.
%%
%% If this assumption doesn't hold, this will fail
%%
-spec insert(pvc_vc(), clog()) -> clog().
insert(VC, C=#clog{at=Id, smallest=bottom, data=Tree}) ->
    Key = pvc_vclock:get_time(Id, VC),
    %% If smallest is bottom, set the first time we get to it
    %% IMPORTANT: We assume that the entries will be added in order
    C#clog{smallest=Key, data=gb_trees:insert(Key, VC, Tree)};

insert(VC, C=#clog{at=Id, smallest=Smallest, data=Tree}) ->
    Key = pvc_vclock:get_time(Id, VC),
    {NewSmallest, NewTree} = maybe_gc(Smallest, gb_trees:insert(Key, VC, Tree)),
    C#clog{smallest=NewSmallest, data=NewTree}.

maybe_gc(Smallest, Tree) ->
    Size = gb_trees:size(Tree),
    case Size > ?GC_THRESHOLD of
        false ->
            {Smallest, Tree};
        true ->
            StartingAt = Smallest,
            %% Here smallest will never be bottom, there's always some data
            Edge = Smallest + (Size - ?KEEP_VERSIONS),
            NewTree = gc_tree(StartingAt, Edge, Tree),
            {Edge, NewTree}
    end.

gc_tree(N, N, Acc) ->
    Acc;

gc_tree(Start, Edge, Acc) when Edge > Start ->
    gc_tree(Start + 1, Edge, gb_trees:delete_any(Start, Acc)).

get_smaller_from_dots(HasRead, InVC, C=#clog{at=P, smallest=Min}) ->
    Selected = get_smaller_from_dots_internal(HasRead, InVC, C),
    SelectedTime = pvc_vclock:get_time(P, Selected),
    case lower(SelectedTime, Min) of
        true ->
            %% If the clock selected is lower than our smallest clock, that
            %% means that the valid clock, if it existed, has been pruned by gc
            antidote_stats_collector:log_clog_miss(P);
        false ->
            ok
    end,
    Selected.

%% Given a VC and a list of indexes to check, get the maximum entry in the log
%% such that it's lower or equal to VC at the given indexes.
%%
%%    max { e \in Clog | forall idx. e[idx] <= VC[idx] }
%%
-spec get_smaller_from_dots_internal([partition_id()], pvc_vc(), clog()) -> pvc_vc().
get_smaller_from_dots_internal([], _, #clog{data=Tree}) ->
    case gb_trees:is_empty(Tree) of
        true ->
            pvc_vclock:new();
        false ->
            element(2, gb_trees:largest(Tree))
    end;

get_smaller_from_dots_internal(Dots, VC, #clog{data=Tree}) ->
    case catch get_smaller_from_dots_internal(Dots, VC, gb_trees:balance(Tree), pvc_vclock:new()) of
        {found, Max} ->
            Max;
        Default ->
            Default
    end.

get_smaller_from_dots_internal(_, _, none, Acc) ->
    Acc;

get_smaller_from_dots_internal(Dots, VC, Tree, Acc) ->
    case get_root(Tree) of
        none ->
            Acc;
        {_K, Value} ->
            case vc_ge_for_dots(Dots, Value, VC) of
                false ->
                    %% Given VC is too large, try in a previous entry
                    get_smaller_from_dots_internal(Dots, VC, left(Tree), Acc);
                true ->
                    %% The visited vector is small enough, so we compare it
                    %% with the running max. If our max doesn't change,
                    %% we're already done
                    Selected = pvc_vclock:max(Acc, Value),
                    case pvc_vclock:eq(Selected, Acc) of
                        true ->
                            throw({found, Selected});
                        false ->
                            get_smaller_from_dots_internal(Dots, VC, right(Tree), Selected)
                    end
            end
    end.

%% Same as vectorclock:ge but only for the given partition ids
%%
%%  vc_ge_for_dots([a, ... , z], A, B) == A[a] =< B[b] ^ .. ^ A[z] =< B[z]
%%
-spec vc_ge_for_dots(list(partition_id()), pvc_vc(), pvc_vc()) -> boolean().
vc_ge_for_dots(Dots, A, B) ->
    Compared = lists:map(fun(Dot) ->
        {pvc_vclock:get_time(Dot, A), pvc_vclock:get_time(Dot, B)}
    end, Dots),
    lists:all(fun({X, Y}) -> X =< Y end, Compared).

%% Util

-spec lower(non_neg_integer(), non_neg_integer() | atom) -> boolean().
lower(_, bottom) -> false;
lower(L, R) -> L < R.

%% Peeked at the internals of gb_trees for this

-spec get_root(gb_trees:tree()) -> {integer(), pvc_vc()} | none.
get_root({_, nil}) ->
    none;

get_root({_, {K, V, _, _}}) ->
    {K, V}.

-spec right(gb_trees:tree()) -> gb_trees:tree() | none.
right({_, nil}) ->
    none;

right({S, {_K, _V, _L, R}}) ->
    {S - 1, R}.

-spec left(gb_trees:tree()) -> gb_trees:tree() | none.
left({_, nil}) ->
    none;

left({S, {_K, _V, L, _R}}) ->
    {S - 1, L}.
