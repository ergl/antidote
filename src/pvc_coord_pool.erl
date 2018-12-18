% -------------------------------------------------------------------
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

-module(pvc_coord_pool).
-behaviour(supervisor).

-include("antidote.hrl").

-export([take_pool_tx/0,
         drop_pool_tx/1]).

-export([start_link/0,
         init/1]).

-define(POOL_NAME, coord_pool).
-define(POOL_SIZE, 1000).
-define(POOL_OVERFLOW, 0).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    PoolOptions = [
        {name, {local, ?POOL_NAME}},
        {worker_module, pvc_coord_pool_worker},
        {size, ?POOL_SIZE},
        {max_overflow, ?POOL_OVERFLOW}
    ],
    PoolSpec = poolboy:child_spec(?POOL_NAME, PoolOptions, []),
    {ok, {{one_for_one, 5, 10}, [PoolSpec]}}.

take_pool_tx() ->
    Pid = poolboy:checkout(?POOL_NAME),
    ok = gen_fsm:sync_send_event(Pid, checkout),
    Pid.

drop_pool_tx(Pid) ->
    ok = gen_fsm:sync_send_event(Pid, checkin),
    ok = poolboy:checkin(?POOL_NAME, Pid),
    ok.
