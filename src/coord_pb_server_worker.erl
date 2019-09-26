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
-include("debug_log.hrl").

-behaviour(gen_server).
-behavior(ranch_protocol).

-record(state, {
    socket :: inet:socket(),
    transport :: module(),
    %% The lenght (in bits) of the message identifier
    %% Identifiers are supposed to be opaque, and are ignored by the server,
    %% and simply forwarded back to the client
    id_len :: non_neg_integer() | undefined
}).

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
    ok = Transport:setopts(Socket, [{active, once}, {packet, 4}]),
    IDLen = application:get_env(antidote, coord_id_len_bits, 16),
    State = #state{socket=Socket, transport=Transport, id_len=IDLen},
    gen_server:enter_loop(?MODULE, [], State).

handle_call(E, From, S) ->
    lager:warning("server got unexpected call with msg ~w from ~w", [E, From]),
    {reply, ok, S}.

handle_cast(E, S) ->
    lager:warning("server got unexpected cast with msg ~w", [E]),
    {noreply, S}.

handle_info({tcp, Socket, Data}, State = #state{socket=Socket,
                                                transport=Transport,
                                                id_len=IDLen}) ->
    <<MessageID:IDLen, Request/binary>> = Data,
    {PBMod, PBType, Msg} = pvc_proto:decode_client_req(Request),

    ?LAGER_LOG("request id=~b type=~p msg=~w", [MessageID, PBType, Msg]),

    Promise = coord_req_promise:new(self(), {MessageID, PBMod, PBType}),
    ok = coord_pb_req_handler:process_request(Promise, PBType, Msg),

    Transport:setopts(Socket, [{active, once}]),
    {noreply, State};

handle_info({tcp_closed, _Socket}, S) ->
    lager:info("server got tcp_closed"),
    {stop, normal, S};

handle_info({tcp_error, _Socket, Reason}, S) ->
    lager:info("server got tcp_error"),
    {stop, Reason, S};

handle_info(timeout, State) ->
    lager:info("server got timeout"),
    {stop, normal, State};

handle_info({promise_resolve, Result, {Id, Mod, Type}}, S=#state{socket=Socket,
                                                    transport=Transport,
                                                    id_len=IDLen}) ->

    ?LAGER_LOG("response id=~b msg=~w", [Id, Result]),
    Reply = pvc_proto:encode_serv_reply(Mod, Type, Result),
    Transport:send(Socket, <<Id:IDLen, Reply/binary>>),
    {noreply, S};

handle_info(E, S) ->
    lager:warning("server got unexpected info with msg ~w", [E]),
    {noreply, S}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_, #state{socket=undefined, transport=undefined}) ->
    ok;

terminate(_Reason, #state{socket=Socket, transport=Transport}) ->
    catch Transport:close(Socket),
    ok.
