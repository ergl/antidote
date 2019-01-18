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

-module(pvc_coord_pool_sup).
-behaviour(supervisor).

-include("antidote.hrl").

-export([start_pool/0]).

%% supervisor callbacks
-export([start_link/0,
         init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    PoolSpec = #{id => undefined,
                 start => {pvc_coord_pool, start_link, []}},
    {ok, {{simple_one_for_one, 5, 10}, [PoolSpec]}}.

start_pool() ->
    supervisor:start_child(?MODULE, []).
