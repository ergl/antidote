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

-module(transactional_protocol).

-include("antidote.hrl").

%% API
-export([
    get_protocol/0,
    start_transaction/2,
    start_transaction/3,
    commit_transaction/1,
    abort_transaction/1,
    read_objects/2,
    read_objects/3,
    read_objects/4,
    update_objects/2,
    update_objects/3,
    update_objects/4
]).

start_transaction(Clock, Properties) ->
    start_transaction(Clock, Properties, false).

start_transaction(Clock, Properties, KeepAlive) ->
    Module = get_protocol_module(),
    Module:start_transaction(Clock, Properties, KeepAlive).

commit_transaction(TxId) ->
    Module = get_protocol_module(),
    Module:commit_transaction(TxId).

abort_transaction(TxId) ->
    Module = get_protocol_module(),
    Module:abort_transaction(TxId).

read_objects(Objects, TxId) ->
    Module = get_protocol_module(),
    Module:read_objects(Objects, TxId).

read_objects(Clock, Properties, Objects) ->
    Module = get_protocol_module(),
    Module:read_objects(Clock, Properties, Objects).

read_objects(Clock, Properties, Objects, KeepAlive) ->
    Module = get_protocol_module(),
    Module:read_objects(Clock, Properties, Objects, KeepAlive).

update_objects(Updates, TxId) ->
    Module = get_protocol_module(),
    Module:update_objects(Updates, TxId).

update_objects(Clock, Properties, Updates) ->
    Module = get_protocol_module(),
    Module:update_objects(Clock, Properties, Updates).

update_objects(Clock, Properties, Updates, KeepAlive) ->
    Module = get_protocol_module(),
    Module:update_objects(Clock, Properties, Updates, KeepAlive).

-spec get_protocol_module() -> transactional_protocol().
get_protocol_module() ->
    {ok, TransactionalModule} = antidote_config:get(?TRANSACTION_CONFIG, clocksi),
    case TransactionalModule of
        gr -> cure;
        clocksi -> cure;
        Protocol -> Protocol
    end.

-spec get_protocol() -> clocksi | gr | pvc.
get_protocol() ->
    {ok, TransactionalModule} = antidote_config:get(?TRANSACTION_CONFIG, clocksi),
    TransactionalModule.
