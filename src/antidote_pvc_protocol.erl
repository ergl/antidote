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
         read_request/4,
         prepare/4,
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
    RefreshReply = dc_utilities:bcast_vnode_sync(clocksi_vnode_master, pvc_refresh_replicas),
    ok = all_ok(RefreshReply),

    %% Force advance replica state
    ForceStateReply = dc_utilities:bcast_vnode_sync(clocksi_vnode_master,
                                                    {pvc_unsafe_set_clock, NewLastPrep, ForceClock}),
    ok = all_ok(ForceStateReply),
    ok.

-ifdef(read_request).
read_request(Partition, Key, VCaggr, HasRead) ->
    Received = os:timestamp(),
    {Took, ok} = timer:tc(pvc_read_replica,
                          async_read,
                          [self(), Partition, Key, HasRead, VCaggr]),

    WaitReceive = os:timestamp(),
    receive
        {error, Reason} ->
            {error, Reason};

        {ok, _Value, CommitVC, MaxVC} ->
            ReceivedMsg = os:timestamp(),
            {ok, #{rcv => Received,
                   read_took => Took,
                   wait_took => timer:now_diff(ReceivedMsg, WaitReceive),
                   send => os:timestamp()}, CommitVC, MaxVC}
    end.
-else.
read_request(Partition, Key, VCaggr, HasRead) ->
    ok = pvc_read_replica:async_read(self(), Partition, Key, HasRead, VCaggr),
    receive
        {error, Reason} ->
            {error, Reason};
        {ok, Value, CommitVC, MaxVC} ->
            {ok, Value, CommitVC, MaxVC}
    end.
-endif.

prepare(Partition, TxId, WriteSet, PartitionVersion) ->
    ok = clocksi_vnode:pvc_prepare(self(), Partition, TxId, WriteSet, PartitionVersion),
    receive
        {error, Reason} ->
            ?LAGER_LOG("Received ~p", [{error, Reason}]),
            {error, Partition, Reason};
        {ok, SeqNumber} ->
            ?LAGER_LOG("Received ~p", [{ok, Partition, SeqNumber}]),
            {ok, Partition, SeqNumber}
    end.

decide(Partition, TxId, Outcome) ->
    clocksi_vnode:pvc_decide(Partition, TxId, Outcome).

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
