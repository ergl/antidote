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
-include("pvc.hrl").

-type vlog() :: {partition_id(), type(), {non_neg_integer(), dict:dict(integer(), {term(), vectorclock()})}}.

%% API
-export([new/2,
         insert/3,
         get_smaller/2]).

-spec new(partition_id(), type()) -> vlog().
new(AtId, Type) ->
    {AtId, Type, {1, dict:new()}}.

-spec insert(vectorclock(), term(), vlog()) -> vlog().
insert(VC, Value, {Id, Type, {Smallest, Dict}}) ->
    Key = vectorclock_partition:get_partition_time(Id, VC),
    {Id, Type, maybe_gc(Smallest, dict:store(Key, {Value, VC}, Dict))}.

maybe_gc(Smallest, Dict) ->
    Size = dict:size(Dict),
    case Size > ?VERSION_THRESHOLD of
        false ->
            {Smallest, Dict};
        true ->
            StartingAt = Smallest,
            Edge = Smallest + (Size - ?MAX_VERSIONS),
            NewTree = gc_dict(StartingAt, Edge, Dict),
            {Edge, NewTree}
    end.

gc_dict(N, N, Acc) ->
    Acc;

gc_dict(S, E, Acc) when E > S ->
    gc_dict(S + 1, E, dict:erase(S, Acc)).

-spec get_smaller(vectorclock(), vlog()) -> {term(), vectorclock()}.
get_smaller(VC, {Id, Type, {_, Dict}}) ->
    case dict:is_empty(Dict) of
        true ->
            base_entry(Type);
        false ->
            LookupKey = vectorclock_partition:get_partition_time(Id, VC),
            get_smaller(LookupKey, Type, Dict)
    end.

-spec get_smaller(non_neg_integer(), type(), dict:dict()) -> {term(), vectorclock()}.
get_smaller(0, Type, _) ->
    base_entry(Type);

get_smaller(LookupKey, Type, Dict) ->
    case dict:find(LookupKey, Dict) of
        error ->
            get_smaller(LookupKey - 1, Type, Dict);

        {ok, Snapshot} ->
            Snapshot
    end.

%% Util

base_entry(Type) ->
    Value = Type:value(Type:new()),
    CommitTime = vectorclock:new(),
    {Value, CommitTime}.
