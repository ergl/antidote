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

-define(bottom, {<<>>, pvc_vclock:new()}).

-record(vlog, {
    at :: partition_id(),
    smallest :: non_neg_integer() | bottom,
    data :: dict:dict(integer(), {val(), pvc_vc()})
}).

-type vlog() :: #vlog{}.

%% API
-export([new/1,
         insert/3,
         get_smaller/2]).

-spec new(partition_id()) -> vlog().
new(AtId) ->
    #vlog{at=AtId, smallest=bottom, data=dict:new()}.

-spec insert(pvc_vc(), term(), vlog()) -> vlog().
insert(VC, Value, V=#vlog{at=Id, smallest=bottom, data=Dict}) ->
    Key = pvc_vclock:get_time(Id, VC),
    V#vlog{smallest=Key, data=dict:store(Key, {Value, VC}, Dict)};

insert(VC, Value, V=#vlog{at=Id, smallest=Smallest, data=Dict}) ->
    Key = pvc_vclock:get_time(Id, VC),
    {NewSmallest, NewDict} = maybe_gc(Smallest, dict:store(Key, {Value, VC}, Dict)),
    V#vlog{smallest = NewSmallest, data=NewDict}.

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

gc_dict(Start, Edge, Acc) when Edge > Start ->
    gc_dict(Start + 1, Edge, dict:erase(Start, Acc)).

-spec get_smaller(pvc_vc(), vlog()) -> {val(), pvc_vc()}.
get_smaller(VC, #vlog{at=Id, smallest=Smallest, data=Dict}) ->
    case dict:is_empty(Dict) of
        true ->
            ?bottom;
        false ->
            LookupKey = pvc_vclock:get_time(Id, VC),
            %% TODO(borja): Test this
            case Smallest =/= bottom andalso LookupKey < Smallest of
                true ->
                    ?bottom;
                false ->
                    get_smaller_internal(LookupKey, Dict)
            end
    end.

-spec get_smaller_internal(non_neg_integer(), dict:dict()) -> {term(), vectorclock()}.
get_smaller_internal(0, _) ->
    ?bottom;

get_smaller_internal(LookupKey, Dict) ->
    case dict:find(LookupKey, Dict) of
        error ->
            get_smaller_internal(LookupKey - 1, Dict);

        {ok, Snapshot} ->
            Snapshot
    end.
