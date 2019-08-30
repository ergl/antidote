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

-include("log_version_miss.hrl").

-behaviour(gen_server).

%% Interval to collect metrics
-define(INTERVAL, 10000). %% 10 sec

%% Called by rpc
-ignore_xref([report_vlog_misses/0]).

%% API
-export([start_link/0,
         init_stale_metrics/0,
         init_vlog_miss_table/0,
         report_vlog_misses/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    update_timer = undefined :: undefined | timer:tref(),
    log_miss_table = undefined :: undefined | ets:tab()
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec init_stale_metrics() -> ok.
init_stale_metrics() ->
    gen_server:cast(?MODULE, init_metrics).

%% @doc Init the version miss table
%%      Will track how many key version misses we do by aggressive GC
-spec init_vlog_miss_table() -> ok.
init_vlog_miss_table() ->
    gen_server:cast(?MODULE, init_vlog_miss_table).

-spec report_vlog_misses() -> [tuple()].
report_vlog_misses() ->
    ets:tab2list(?LOG_MISS_TABLE).

init([]) ->
    {ok, #state{}}.

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

handle_cast(init_vlog_miss_table, State=#state{log_miss_table=undefined}) ->
    lager:info("Initializing ~p ETS table", [?LOG_MISS_TABLE]),
    Table = ets:new(?LOG_MISS_TABLE, [set, named_table, public, {write_concurrency, true}]),
    {noreply, State#state{log_miss_table=Table}};

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
