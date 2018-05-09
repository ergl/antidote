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

-module(rubis_pb_worker).
-behaviour(gen_server).

-record(state, { socket }).

-export([start_link/1]).

-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2]).

start_link(Socket) ->
    gen_server:start_link(?MODULE, Socket, []).

init(Socket) ->
    gen_server:cast(self(), accept),
    {ok, #state{socket=Socket}}.

handle_call(_E, _From, State) ->
    {noreply, State}.

handle_cast(accept, S = #state{socket = ListenSocket}) ->
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    rubis_pb_sup:start_socket(),
    ok = inet:setopts(AcceptSocket, [{active, once}]),
    {noreply, S#state{socket=AcceptSocket}}.

handle_info({tcp, _Socket, Data}, State = #state{socket = Sock}) ->
    {HandlerMod, Type, Msg} = rubis_proto:decode_client_req(Data),
    Result = rubis:process_request(Type, Msg),
%%    lager:info("Processed pb request ~p with result ~p", [Type, Result]),
    Reply = rubis_proto:encode_serv_reply(HandlerMod, Type, Result),
    gen_tcp:send(Sock, Reply),
    ok = inet:setopts(Sock, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    {stop, normal, S};

handle_info({tcp_error, _Socket, _}, S) ->
    {stop, normal, S};

handle_info(E, S) ->
    io:format("unexpected: ~p~n", [E]),
    {noreply, S}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(normal, _State) ->
    ok;

terminate(_Reason, _State) ->
    io:format("terminate reason: ~p~n", [_Reason]).
