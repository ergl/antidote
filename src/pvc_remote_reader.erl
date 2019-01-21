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
-module(pvc_remote_reader).
-behavior(gen_server).

-include("antidote.hrl").
-include("pvc.hrl").

%% Supervision tree callbacks
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% API
-export([name/1,
         async_remote_read/4]).

-record(state, {
    sockets :: orddict:orddict(node(), gen_tcp:socket())
}).

-spec start_link(non_neg_integer()) -> {ok, pid()} | {error, term()}.
start_link(Id) ->
    gen_server:start_link({local, name(Id)}, ?MODULE, [], []).

-spec name(non_neg_integer()) -> atom().
name(Id) ->
    BinId = integer_to_binary(Id),
    Name = <<"remote_reader_", BinId/binary>>,
    try
        binary_to_existing_atom(Name, latin1)
    catch _:_Reason ->
        binary_to_atom(Name, latin1)
    end.

-spec random_name() -> atom().
random_name() ->
    name(rand:uniform(?NUM_READERS)).

-spec async_remote_read(index_node(), key(), sets:set(), pvc_vc()) -> ok.
async_remote_read(IndexNode, Key, HasRead, VCaggr) ->
    gen_server:cast(random_name(), {read_from, IndexNode, self(), {#{remote_read_server => os:timestamp()}, Key}, HasRead, VCaggr}).

init([]) ->
    Self = node(),
    RemotePort = ?RUBIS_PB_PORT,
    Options = [binary, {active, false}, {packet, 2}, {nodelay, true}],
    AllDCNodes = dc_utilities:get_my_dc_nodes(),
    Sockets = lists:foldl(fun(Node, Acc) ->
        case Node of
            Self ->
                Acc;
            _ ->
                [_, BinIp] = binary:split(atom_to_binary(Node, latin1), <<"@">>),
                IP = binary_to_atom(BinIp, latin1),
                {ok, Sock} = gen_tcp:connect(IP, RemotePort, Options),
                orddict:store(Node, Sock, Acc)
        end
    end, orddict:new(), AllDCNodes),
    {ok, #state{sockets = Sockets}}.

handle_call(stop, _Sender, State) ->
    {stop, shutdown, ok, State};

handle_call(Msg, From, State) ->
    lager:info("Unrecognized msg ~p from ~p", [Msg, From]),
    {noreply, State}.

handle_cast({read_from, {Partition, Node}, From, {InfoMap, Key}, HasRead, VCaggr}, State=#state{sockets=Socks}) ->
    Rcv = os:timestamp(),
    {ok, Socket} = orddict:find(Node, Socks),
    Msg = rpb_simple_driver:remote_read(Partition, Key, HasRead, VCaggr),
    SendSock = os:timestamp(),
    ok = gen_tcp:send(Socket, Msg),
    {ok, BinReply} = gen_tcp:recv(Socket, 0),
    RcvSock = os:timestamp(),
    case rubis_proto:decode_serv_reply(BinReply) of
        {error, Reason} ->
            gen_fsm:send_event(From, {error, Reason});
        {ok, {Value, CommitVC, MaxVC}} ->
            InfoMap1 = maps:update_with(remote_read_server, fun(T) -> timer:now_diff(Rcv, T) end, InfoMap),
            InfoMap2 = InfoMap1#{remote_read_socket => timer:now_diff(RcvSock, SendSock), fsm_diff => os:timestamp()},
            gen_fsm:send_event(From, {pvc_readreturn, Partition, {InfoMap2, Key}, Value, CommitVC, MaxVC})
    end,
    {noreply, State};

handle_cast(Request, State) ->
    lager:info("Unrecognized msg ~p", [Request]),
    {noreply, State}.

handle_info(Info, State) ->
    lager:info("Unrecognized msg ~p", [Info]),
    {noreply, State}.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

terminate(_Reason, #state{sockets = Socks}) ->
    lists:foreach(fun({_Node, Sock}) -> gen_tcp:close(Sock) end, Socks),
    ok.
