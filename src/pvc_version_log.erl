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

%% When the log reaches this size, the log will be pruned
-define(GC_THRESHOLD, 30).
%% The number of versions to keep after a GC pass
-define(KEEP_VERSIONS, 15).

-define(bottom, {<<>>, pvc_vclock:new()}).

-type min_version() :: undefined | non_neg_integer().
-type versions() :: orddict:dict(neg_integer(), {val(), pvc_vc()}).

-record(vlog, {
    %% The partition where this structure resides
    at :: partition_id(),
    %% The actual version list
    %% The data is structured as an ordered dict where the value is a
    %% snapshot, and the key, the time of that snapshot at this partition.
    %%
    %% This snapshot time is stored as a negative number, as the default
    %% ordered dict implementation orders them in ascending order, and
    %% we want them in descending order.
    data :: versions(),
    %% The minimum version we hold, useful to short-circuit searching
    %% the array of versions if we are asked for a version we no longer
    %% hold in memory. This is needed because we garbage collect old
    %% versions as the log gets bigger. This helps us track how many
    %% versions we should keep around.
    min_version :: min_version()
}).

-type vlog() :: #vlog{}.

%% API
-export([new/1,
         insert/3,
         get_smaller/2]).

-spec new(partition_id()) -> vlog().
new(AtId) ->
    #vlog{at=AtId, data=orddict:new(), min_version=undefined}.

-spec insert(pvc_vc(), term(), vlog()) -> vlog().
insert(VC, Value, V=#vlog{at=Id, data=Dict, min_version=OldMin}) ->
    Key = pvc_vclock:get_time(Id, VC),
    {NewData, NewMin} = maybe_gc(Key,
                                 OldMin,
                                 orddict:store(-Key, {Value, VC}, Dict)),
    V#vlog{data=NewData, min_version=NewMin}.

-spec maybe_gc(non_neg_integer(), min_version(), versions()) -> {versions(), non_neg_integer()}.
maybe_gc(LastInserted, OldMin, Data) ->
    Size = orddict:size(Data),
    case Size > ?GC_THRESHOLD of
        false ->
            {Data, min_version(LastInserted, OldMin)};
        true ->
            %% TODO(borja): Improve this, both sublist and last are O(n)
            %% Merge sublist and last (and maybe the min version calc)
            %% into a single function
            NewData = lists:sublist(Data, ?KEEP_VERSIONS),
            {PrunedMinVersion, _} = lists:last(NewData),
            NewMin = max_version(abs(PrunedMinVersion), OldMin),
            {NewData, NewMin}
    end.

-spec min_version(non_neg_integer(), min_version()) -> non_neg_integer().
min_version(Left, undefined) -> Left;
min_version(Left, Right) -> erlang:min(Left, Right).

-spec max_version(non_neg_integer(), min_version()) -> non_neg_integer().
max_version(Left, undefined) -> Left;
max_version(Left, Right) -> erlang:max(Left, Right).

-spec get_smaller(pvc_vc(), vlog()) -> {val(), pvc_vc()}.
get_smaller(_VC, #vlog{data=[]}) ->
    ?bottom;

get_smaller(VC, #vlog{at=Id, data=[{MaxTime, MaxVersion} | _]=Data, min_version=MinVersion}) when MinVersion =/= undefined ->
    LookupKey = pvc_vclock:get_time(Id, VC),
    if
        LookupKey > abs(MaxTime) ->
            MaxVersion;
        LookupKey < MinVersion ->
            ok = antidote_stats_collector:log_version_miss(Id),
            ?bottom;
        true ->
            get_smaller_internal(-LookupKey, Data)
    end.

-spec get_smaller_internal(neg_integer(), versions()) -> {val(), pvc_vc()}.
get_smaller_internal(_, []) ->
    ?bottom;

get_smaller_internal(LookupKey, [{Time, Version} | Rest]) ->
    case LookupKey =< Time of
        true ->
            Version;
        false ->
            get_smaller_internal(LookupKey, Rest)
    end.
