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

%%@doc: This module periodically collects different metrics (currently only staleness)
%%  and updates exometer metrics. Monitoring tools can then read it from exometer.

-module(antidote_stats_collector).

-include("antidote.hrl").

-behaviour(gen_server).

%% Interval to collect metrics
-define(INTERVAL, 10000). %% 10 sec

%% Called by rpc
-ignore_xref([report_stats/0]).

%% API
-export([start_link/0,
         init_stale_metrics/0,
         log_version_miss/1,
         log_clog_miss/1,
         log_fix_vc_miss/1,
         log_partition_not_ready/1,
         report_stats/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(stat_entry, {
    partition :: partition_id(),
    vlog_misses = 0 :: non_neg_integer(),
    clog_misses = 0 :: non_neg_integer(),
    fix_vc_misses = 0 :: non_neg_integer(),
    not_ready_tries = 0 :: non_neg_integer()
}).

-record(state, {
    update_timer = undefined :: undefined | timer:tref(),
    stats_table :: ets:tab()
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init_stale_metrics() -> ok.
init_stale_metrics() ->
    gen_server:cast(?MODULE, init_metrics).

-spec log_version_miss(partition_id()) -> ok.
log_version_miss(Partition) ->
    incr_counter(Partition, #stat_entry.vlog_misses).

-spec log_clog_miss(partition_id()) -> ok.
log_clog_miss(Partition) ->
    incr_counter(Partition, #stat_entry.clog_misses).

-spec log_fix_vc_miss(partition_id()) -> ok.
log_fix_vc_miss(Partition) ->
    incr_counter(Partition, #stat_entry.fix_vc_misses).

-spec log_partition_not_ready(partition_id()) -> ok.
log_partition_not_ready(Partition) ->
    incr_counter(Partition, #stat_entry.not_ready_tries).

-spec report_stats() -> [#{}].
report_stats() ->
    lists:map(fun entry_to_map/1, ets:tab2list(?MODULE)).

init([]) ->
    lager:info("Initializing ~p ETS table", [?MODULE]),
    Table = ets:new(?MODULE, [set,
                              named_table,
                              public,
                              {write_concurrency, true},
                              {keypos, #stat_entry.partition}]),

    {ok, #state{stats_table=Table}}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(init_metrics, State=#state{update_timer=undefined}) ->
    ok = init_metrics(),
    {ok, Timer} = timer:send_interval(?INTERVAL, self(), periodic_update),
    {noreply, State#state{update_timer=Timer}};

handle_cast({update, Metric}, State) ->
    Val = antidote_stats:get_value(Metric),
    exometer:update(Metric, Val),
    {noreply, State};

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(periodic_update, State) ->
    update([staleness]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

update(Metric) ->
    Val = antidote_stats:get_value(Metric),
    exometer:update(Metric, Val).

init_metrics() ->
    Metrics = antidote_stats:stats(),
    lists:foreach(fun(Metric) ->
        exometer:new(Metric, histogram, [{time_span, timer:seconds(60)}])
    end, Metrics).

-spec incr_counter(partition_id(), non_neg_integer()) -> ok.
incr_counter(P, Pos) ->
    _ = ets:update_counter(?MODULE, P, {Pos, 1}, #stat_entry{partition=P}),
    ok.

-spec entry_to_map(#stat_entry{}) -> #{atom() => term()}.
entry_to_map(Entry) ->
    FieldNames = record_info(fields, stat_entry),
    [_Name | Fields] = tuple_to_list(Entry),
    maps:from_list(lists:zip(FieldNames, Fields)).
