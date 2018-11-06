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

-module(rubis_pb_server).

-export([start_listeners/0]).

%% The number of acceptor processes
%% for the RUBIS Protocol Buffer server
-define(RUBIS_NSUP, 50).
-define(DEFAULT_RUBIS_PB_PORT, 7878).

-spec start_listeners() -> ok.
start_listeners() ->
    RubisPbPort = application:get_env(antidote, rubis_pb_port, ?DEFAULT_RUBIS_PB_PORT),
    {ok, _} = ranch:start_listener(tcp_rubis,
                                   ?RUBIS_NSUP,
                                   ranch_tcp,
                                   [{port, RubisPbPort}, {max_connections, infinity}],
                                   rubis_pb_worker,
                                   []),

    Port = ranch:get_port(tcp_rubis),
    lager:info("Rubis pb server started on port ~p", [Port]),

    ok.
