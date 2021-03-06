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

-module(coord_pb_server).

-include("pvc.hrl").

-export([start_listeners/0]).

-spec start_listeners() -> ok.
start_listeners() ->
    DefaultPort = application:get_env(antidote, coord_pb_port, ?COORD_PB_PORT),
    {ok, _} = ranch:start_listener(tcp_coord_conn,
                                   ?COORD_PB_POOL,
                                   ranch_tcp,
                                   [{port, DefaultPort}, {max_connections, infinity}],
                                   coord_pb_server_worker,
                                   []),

    Port = ranch:get_port(tcp_coord_conn),
    lager:info("Rubis pb server started on port ~p", [Port]),

    ok.
