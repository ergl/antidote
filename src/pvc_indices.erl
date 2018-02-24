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

%% API
-export([u_index/4,
         read_u_index/3,
         index/4]).

u_index(IndexName, IndexValue, RefKey, TxId) ->
    Key = make_u_index_key(IndexName, IndexValue),
    pvc:update_keys({Key, RefKey}, TxId).

read_u_index(IndexName, IndexValue, TxId) ->
    Key = make_u_index_key(IndexName, IndexValue),
    pvc:read_keys(Key, TxId).

index(IndexName, IndexValue, RefKey, TxId) ->
    RootKey = make_u_index_key(IndexName, IndexValue),
    IndexKey = make_index_key(IndexName, IndexValue, RefKey),
    {ok, [Val]} = pvc:read_keys(RootKey, TxId),
    Updates = case Val of
        <<>> ->
            [{RootKey, taken}, {IndexKey, RefKey}];
        taken ->
            [{IndexKey, RefKey}]
    end,
    update_indices(Updates, TxId).

update_indices(Updates, TxId = #tx_id{server_pid = Pid}) ->
    ok = pvc:update_keys(Updates, TxId),
    gen_fsm:sync_send_event(Pid, {pvc_index, Updates}, ?OP_TIMEOUT).

%% TODO(borja): Handle non-binary data
make_u_index_key(IndexName, IndexValue) ->
    <<IndexName/binary, IndexValue/binary>>.

%% TODO(borja): Handle non-binary data
make_index_key(IndexName, IndexValue, RefKey) ->
    <<IndexName/binary, IndexValue/binary, RefKey/binary>>.
