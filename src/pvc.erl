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

-module(pvc).

-include("antidote.hrl").

%% PVC-Only API
-export([start_transaction/0,
         read/2,
         read_batch/2,
         update/3,
         update_batch/2,
         commit_transaction/1]).

%% Unsafe load API
-export([unsafe_load/2]).

%% New FSM API

-spec start_transaction() -> {ok, txid()} | {error, reason()}.
start_transaction() ->
    {ok, _} = pvc_coord_sup:start_fsm([self()]),
    receive
        {ok, TxId} -> {ok, TxId};
        Err -> {error, Err}
    end.

-spec read(key(), txid()) -> {ok, val()} | {error, reason()}.
read(Key, #tx_id{server_pid=Pid}) ->
    gen_fsm:sync_send_event(Pid, {read, Key}, ?OP_TIMEOUT).

-spec read_batch([key()], txid()) -> {ok, [val()]} | {error, reason()}.
read_batch(Keys, #tx_id{server_pid=Pid}) ->
    gen_fsm:sync_send_event(Pid, {read_batch, Keys}, ?OP_TIMEOUT).

-spec update_batch([{key(), val()}], txid()) -> ok.
update_batch(Updates, #tx_id{server_pid=Pid}) ->
    gen_fsm:sync_send_event(Pid, {update_batch, Updates}, ?OP_TIMEOUT).

-spec update(key(), val(), txid()) -> ok.
update(Key, Val, #tx_id{server_pid=Pid}) ->
    gen_fsm:sync_send_event(Pid, {update, Key, Val}, ?OP_TIMEOUT).

-spec commit_transaction(txid()) -> ok | {error, reason()}.
commit_transaction(#tx_id{server_pid=Pid}) ->
    gen_fsm:sync_send_event(Pid, commit, ?OP_TIMEOUT).

%% @doc UNSAFE: Blindly write a random binary blobs of size Size to N keys
%%
%% NOTE: Don't use outside of benchmarks, as this sidesteps the read-before
%% write mechanism, and won't play well with concurrent transactions.
%%
%% The keys that are updated are integer_to_binary(1, 36) .. integer_to_binary(N, 36)
unsafe_load(N, Size) ->
    case start_transaction() of
        {ok, #tx_id{server_pid = Pid}} ->
            gen_fsm:sync_send_event(Pid, {unsafe_load, N, Size}, ?OP_TIMEOUT);
        Err ->
            {error, Err}
    end.
