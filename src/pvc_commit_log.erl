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

-type clog() :: {partition_id(), gb_sets:tree(non_neg_integer(), vectorclock())}.

-export_type([clog/0]).

-export([new_at/1,
         insert/2,
         get_smaller_from_dots/3]).

-spec new_at(partition_id()) -> clog().
new_at(AtId) ->
    {AtId, gb_trees:empty()}.

%% The Commit Log at the i-th partition (CLog_i) is only updated
%% when if there is a new transaction committed at i. This means
%% that the entries in CLog_i are strictly monotonic at their i-th
%% entry. We can use this to store the entries in a tree, where the
%% key is the i-th value of an entry.
%%
%% If this assumption doesn't hold, this will fail
%%
-spec insert(vectorclock_partition:partition_vc(), clog()) -> clog().
insert(VC, {Id, Tree}) ->
    Key = vectorclock_partition:get_partition_time(Id, VC),
    lager:info("[~p] Inserting ~p at ~p", [Id, dict:to_list(VC), Key]),
    {Id, gb_trees:insert(Key, VC, Tree)}.

%% Given a VC and a list of indexes to check, get the maximum entry in the log
%% such that it's lower or equal to VC at the given indexes.
%%
%%    max { e \in Clog | forall idx. e[idx] <= VC[idx] }
%%
-spec get_smaller_from_dots([partition_id()], vectorclock(), clog()) -> vectorclock().
get_smaller_from_dots([], _, {_, Tree}) ->
    case gb_trees:is_empty(Tree) of
        true ->
            vectorclock:new();
        false ->
            element(2, gb_trees:largest(Tree))
    end;

get_smaller_from_dots(Dots, VC, {_, Tree}) ->
    case catch get_smaller_from_dots(Dots, VC, gb_trees:balance(Tree), vectorclock_partition:new()) of
        {found, Max} ->
            lager:info("get_smaller_from_dots found ~p", [dict:to_list(Max)]),
            Max;
        Default ->
            lager:info("get_smaller_from_dots defaulted ~p", [dict:to_list(Default)]),
            Default
    end.

get_smaller_from_dots(_, _, none, Acc) ->
    Acc;

get_smaller_from_dots(Dots, VC, Tree, Acc) ->
    case get_root(Tree) of
        none ->
            Acc;
        {K, Value} ->
            lager:info("smaller_from_dots comparing at ~p", [K]),
            case vc_ge_for_dots(Dots, Value, VC) of
                false ->
                    lager:info("Too big, going left"),
                    %% Given VC is too large, try in a previous entry
                    get_smaller_from_dots(Dots, VC, left(Tree), Acc);
                true ->
                    %% The visited vector is small enough, so we compare it
                    %% with the running max. If our max doesn't change,
                    %% we're already done
                    Selected = vectorclock:max([Acc, Value]),
                    case vectorclock:eq(Selected, Acc) of
                        true ->
                            lager:info("Too small, raise found"),
                            throw({found, Selected});
                        false ->
                            lager:info("Small enough, acc and going rigth"),
                            get_smaller_from_dots(Dots, VC, right(Tree), Selected)
                    end
            end
    end.

%% Same as vectorclock:ge but only for the given partition ids
%%
%%  vc_ge_for_dots([a, ... , z], A, B) == A[a] =< B[b] ^ .. ^ A[z] =< B[z]
%%
-spec vc_ge_for_dots(list(partition_id()), vectorclock(), vectorclock()) -> boolean().
vc_ge_for_dots(Dots, A, B) ->
    Compared = lists:map(fun(Dot) ->
        {vectorclock:get_clock_of_dc(Dot, A), vectorclock:get_clock_of_dc(Dot, B)}
    end, Dots),
    lists:all(fun({X, Y}) -> X =< Y end, Compared).

%% Util

%% Peeked at the internals of gb_trees for this

-spec get_root(gb_trees:tree()) -> {integer(), vectorclock()} | none.
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
