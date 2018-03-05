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

-module(rubis_pb_sup).
-behaviour(supervisor).

-export([start_link/0, start_socket/0]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, RubisPbPort} = antidote_config:get(rubis_pb_port),
    {ok, LSock} = gen_tcp:listen(RubisPbPort, [binary, {active, once}, {packet, 2}]),
    {ok, Port} = inet:port(LSock),
    io:format("RUBIS Pb interface listening on port ~p~n", [Port]),
    spawn_link(fun empty_listeners/0),
    {ok, {{simple_one_for_one, 60, 3600},
         [{socket,
          {rubis_pb_worker, start_link, [LSock]},
          temporary, 1000, worker, [rubis_pb_worker]}]}}.

start_socket() ->
    supervisor:start_child(?MODULE, []).

empty_listeners() ->
    {ok, RubisPbListeners} = antidote_config:get(rubis_pb_listeners),
    [start_socket() || _ <- lists:seq(0, RubisPbListeners)],
    ok.

