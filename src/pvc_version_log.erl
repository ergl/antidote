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

-module(pvc_version_log).

-include("antidote.hrl").

-type vlog() :: {partition_id(), type(), gb_sets:tree(integer(), {term(), vectorclock()})}.

%% API
-export([new/2,
         insert/3,
         get_smaller/2,
         to_list/1]).

-spec new(partition_id(), type()) -> vlog().
new(AtId, Type) ->
    {AtId, Type, gb_trees:empty()}.

%% @doc The Version Log at the i-th partition (VLog_i) is only updated
%% when if there is a new transaction committed at i. This means
%% that the entries in VLog_i are strictly monotonic at their i-th
%% entry. We can use this to store the entries in a tree, where the
%% key is the i-th value of an entry.
%%
%% If this assumption doesn't hold, this will fail
%%
-spec insert(vectorclock(), term(), vlog()) -> vlog().
insert(VC, Value, {Id, Type, Tree}) ->
    Key = entry_to_key(Id, VC),
    {Id, Type, gb_trees:insert(Key, {Value, VC}, Tree)}.

-spec get_smaller(vectorclock(), vlog()) -> {term(), vectorclock()}.
get_smaller(VC, {Id, Type, Tree}) ->
    case gb_trees:is_empty(Tree) of
        true ->
            base_entry(Type);
        false ->
            get_smaller(Type, VC, Id, Tree)
    end.

-spec get_smaller(type(), vectorclock(), partition_id(), gb_trees:tree()) -> {term(), vectorclock()}.
get_smaller(Type, VC, Id, Tree) ->
    LookupKey = entry_to_key(Id, VC),
    Iter = gb_trees:iterator_from(LookupKey, Tree),
    %% Will always return something, tree is not empty
    %% iterator_from/2 always returns the first key greater than or equal to
    %% Key is returned. In our case, that's either our snapshot or the previous
    %% one, which is what we want.
    %%
    %% If the only snapshot present is something newer than the one we're
    %% asking for, return the base snapshot
    case gb_trees:next(Iter) of
        none ->
            base_entry(Type);
        {_Key, Snapshot={_Value, _CT}, _} ->
            Snapshot
    end.

to_list({_, _, Tree}) ->
    gb_trees:to_list(Tree).


%% Util

%% @doc Tree key for the snapshot.
%%
%%      We order the tree from newest (left) to oldest (right)
entry_to_key(Partition, VC) ->
    0 - vectorclock_partition:get_partition_time(Partition, VC).

base_entry(Type) ->
    Value = Type:value(Type:new()),
    CommitTime = vectorclock:new(),
    {Value, CommitTime}.
