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

-module(pvc_indices).

-include("antidote.hrl").

-define(CLAIMED, claimed).
-define(KEY_SEP, <<"$">>/binary).

%% API
-export([u_index/4,
         read_u_index/3,
         index/4,
         read_index/3]).

-spec u_index(binary(), binary(), binary(), txid()) -> ok.
u_index(IndexName, IndexValue, RefKey, TxId) ->
    Key = make_u_index_key(IndexName, IndexValue),
    pvc:update_keys({Key, RefKey}, TxId).

-spec read_u_index(binary(), binary(), txid()) -> {ok, list()} | {error, reason()}.
read_u_index(IndexName, IndexValue, TxId) ->
    Key = make_u_index_key(IndexName, IndexValue),
    pvc:read_keys(Key, TxId).

-spec index(binary(), binary(), binary(), txid()) -> ok.
index(IndexName, IndexValue, RefKey, TxId) ->
    RootKey = make_root_index_key(IndexName, IndexValue),
    IndexKey = make_index_key(IndexName, IndexValue, RefKey),
    Updates = case claimed_index(RootKey, TxId) of
        false ->
            [{RootKey, ?CLAIMED}, {IndexKey, RefKey}];

        true ->
            [{IndexKey, RefKey}]
    end,
    update_indices(Updates, TxId).

-spec read_index(binary(), binary(), txid()) -> {ok, list()} | {error, reason()}.
read_index(IndexName, IndexValue, TxId) ->
    RootKey = make_root_index_key(IndexName, IndexValue),
    case claimed_index(RootKey, TxId) of
        false ->
            {ok, []};
        true ->
            {ok, Range} = read_index_range(RootKey, TxId),
            pvc:read_keys(Range, TxId)
    end.

%% Util functions

claimed_index(RootKey, TxId) ->
    {ok, [RootVal]} = pvc:read_keys(RootKey, TxId),
    RootVal =:= ?CLAIMED.

%% TODO(borja): Handle non-binary data
make_u_index_key(IndexName, IndexValue) ->
    <<<<"u_index">>/binary,
        ?KEY_SEP,
        IndexName/binary,
        ?KEY_SEP,
        IndexValue/binary>>.

make_root_index_key(IndexName, IndexValue) ->
    <<<<"index">>/binary,
        ?KEY_SEP,
        IndexName/binary,
        ?KEY_SEP,
        IndexValue/binary>>.

%% TODO(borja): Handle non-binary data
make_index_key(IndexName, IndexValue, RefKey) ->
    <<<<"index">>/binary,
        ?KEY_SEP,
        IndexName/binary,
        ?KEY_SEP,
        IndexValue/binary,
        ?KEY_SEP, RefKey/binary>>.

update_indices(Updates, TxId = #tx_id{server_pid = Pid}) ->
    ok = pvc:update_keys(Updates, TxId),
    gen_fsm:sync_send_event(Pid, {pvc_index, Updates}, ?OP_TIMEOUT).

read_index_range(RootKey, #tx_id{server_pid = Pid}) ->
    PrefixLen = bit_size(RootKey),
    <<Prefix:PrefixLen, _/binary>> = RootKey,
    gen_fsm:sync_send_event(Pid, {pvc_scan_range, {RootKey, Prefix, PrefixLen}}).
