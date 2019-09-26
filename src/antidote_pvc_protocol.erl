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

-module(antidote_pvc_protocol).
-include("debug_log.hrl").

%% New API
-export([connect/0,
         load/1,
         read_request/5,
         prepare/5,
         decide/3]).

%% Old API, deprecated
-export([start_transaction/0,
         read_keys/2,
         update_keys/2,
         commit_transaction/1]).

%% @doc Get the raw ring at this node
connect() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    {ok, riak_core_ring:chash(Ring)}.

%% @doc Force advance the system. Will propagate to all nodes in the ring
-spec load(non_neg_integer()) -> ok.
load(Size) ->
    NewLastPrep = 1,
    Val = crypto:strong_rand_bytes(Size),

    %% Brute-force a new base VC
    ForceClock = lists:foldl(fun(Partition, Acc) ->
        pvc_vclock:set_time(Partition, NewLastPrep, Acc)
    end, pvc_vclock:new(), dc_utilities:get_all_partitions()),

    BottomValue = {Val, ForceClock},

    %% Set the default value for reads
    SetDefaultReply = dc_utilities:bcast_vnode_sync(materializer_vnode_master,
                                                    {pvc_set_default, BottomValue}),
    ok = all_ok(SetDefaultReply),

    %% Refresh all read replicas
    RefreshReply = dc_utilities:bcast_vnode_sync(antidote_pvc_vnode_master, refresh_replicas),
    ok = all_ok(RefreshReply),

    %% Force advance replica state
    ForceStateReply = dc_utilities:bcast_vnode_sync(antidote_pvc_vnode_master,
                                                    {unsafe_set_clock, NewLastPrep, ForceClock}),
    ok = all_ok(ForceStateReply),
    ok.

-ifdef(read_request).
%% FIXME(borja): This will be sync, can we remove?
read_request(Promise, Partition, Key, VCaggr, HasRead) ->
    Received = os:timestamp(),
    {Took, ok} = timer:tc(pvc_read_replica,
                          async_read,
                          [self(), Partition, Key, HasRead, VCaggr]),

    WaitReceive = os:timestamp(),
    receive
        {error, Reason} ->
            coord_req_promise:resolve({error, Reason}, Promise);

        {ok, _Value, CommitVC, MaxVC} ->
            ReceivedMsg = os:timestamp(),
            Reply = {ok, #{rcv => Received,
                           read_took => Took,
                           wait_took => timer:now_diff(ReceivedMsg, WaitReceive),
                           send => os:timestamp()}, CommitVC, MaxVC},
            coord_req_promise:resolve(Reply, Promise)
    end.
-else.
read_request(Promise, Partition, Key, VCaggr, HasRead) ->
    ok = pvc_read_replica:async_read(Promise, Partition, Key, HasRead, VCaggr).
-endif.

prepare(Partition, Protocol, TxId, Payload, PartitionVersion) ->
    Vote = antidote_pvc_vnode:prepare(Partition, Protocol, TxId, Payload, PartitionVersion),
    case Vote of
        {error, Reason} ->
            ?LAGER_LOG("Received ~p", [{error, Reason}]),
            {error, Partition, Reason};
        {ok, SeqNumber} ->
            ?LAGER_LOG("Received ~p", [{ok, Partition, SeqNumber}]),
            {ok, Partition, SeqNumber}
    end.

decide(Partition, TxId, Outcome) ->
    antidote_pvc_vnode:decide(Partition, TxId, Outcome).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all_ok(Replies) ->
    lists:foreach(fun({_, ok}) -> ok end, Replies).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% @deprecated
start_transaction() -> erlang:error(gone).

%% @deprecated
commit_transaction(_) -> erlang:error(gone).

%% @deprecated
read_keys(_, _) -> erlang:error(gone).

%% @deprecated
update_keys(_, _) -> erlang:error(gone).
