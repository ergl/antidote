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

-module(coord_pb_server_worker).

-behaviour(gen_server).
-behavior(ranch_protocol).

-record(state, { socket, transport }).

%% ranch_protocol callback
-export([start_link/4]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

%% Ranch workaround for gen_server
start_link(Ref, Socket, Transport, Opts) ->
    {ok, proc_lib:spawn_link(?MODULE, init, [{Ref, Socket, Transport, Opts}])}.

init({Ref, Socket, Transport, _Opts}) ->
    ok = ranch:accept_ack(Ref),
    ok = ranch:remove_connection(Ref),
    ok = Transport:setopts(Socket, [{active, once}, {packet, 2}]),
    gen_server:enter_loop(?MODULE, [], #state{socket=Socket, transport=Transport}).

handle_call(E, _From, S) ->
    io:format("unexpected call: ~p~n", [E]),
    {reply, ok, S}.

handle_cast(E, S) ->
    io:format("unexpected cast: ~p~n", [E]),
    {noreply, S}.

handle_info({tcp, Socket, Data}, State = #state{socket=Socket, transport=Transport}) ->
    {HandlerMod, Type, Msg} = pvc_proto:decode_client_req(Data),
    %% TODO(borja): Not all process will need to reply (e.g. decide)
    Result = rubis:process_request(Type, Msg),
    Reply = pvc_proto:encode_serv_reply(HandlerMod, Type, Result),
    Transport:send(Socket, Reply),
    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S) ->
    {stop, Reason, S};

handle_info(timeout, State) ->
    {stop, normal, State};

handle_info(E, S) ->
    io:format("unexpected info: ~p~n", [E]),
    {noreply, S}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_, #state{socket=undefined, transport=undefined}) ->
    ok;

terminate(_Reason, #state{socket=Socket, transport=Transport}) ->
    catch Transport:close(Socket),
    ok.
