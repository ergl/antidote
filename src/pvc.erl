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

%% For types key, val, reason
-include("antidote.hrl").
-include("pvc.hrl").

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

-spec start_transaction() -> {ok, txn_id()} | {error, reason()}.
start_transaction() ->
    {ok, _} = pvc_coord_sup:start_fsm([self()]),
    receive
        {ok, TxId} -> {ok, TxId};
        Err -> {error, Err}
    end.

-spec read(key(), txn_id()) -> {ok, val()} | {error, reason()}.
read(Key, #txn_id{server_pid=Pid}) ->
    gen_fsm:sync_send_event(Pid, {read, Key}, infinity).

-spec read_batch([key()], txn_id()) -> {ok, [val()]} | {error, reason()}.
read_batch(Keys, #txn_id{server_pid=Pid}) ->
    gen_fsm:sync_send_event(Pid, {read_batch, Keys}, infinity).

-spec update_batch([{key(), val()}], txn_id()) -> ok.
update_batch(Updates, #txn_id{server_pid=Pid}) ->
    gen_fsm:sync_send_event(Pid, {update_batch, Updates}, infinity).

-spec update(key(), val(), txn_id()) -> ok.
update(Key, Val, #txn_id{server_pid=Pid}) ->
    gen_fsm:sync_send_event(Pid, {update, Key, Val}, infinity).

-spec commit_transaction(txn_id()) -> ok | {error, reason()}.
commit_transaction(#txn_id{server_pid=Pid}) ->
    gen_fsm:sync_send_event(Pid, commit, infinity).

%% @doc UNSAFE: Blindly write a random binary blobs of size Size to N keys
%%
%% NOTE: Don't use outside of benchmarks, as this sidesteps the read-before
%% write mechanism, and won't play well with concurrent transactions.
%%
%% The keys that are updated are integer_to_binary(1, 36) .. integer_to_binary(N, 36)
unsafe_load(N, Size) ->
    case start_transaction() of
        {ok, #txn_id{server_pid = Pid}} ->
            gen_fsm:sync_send_event(Pid, {unsafe_load, N, Size}, infinity);
        Err ->
            {error, Err}
    end.
