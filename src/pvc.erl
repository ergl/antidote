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

%% API
-export([
    start_transaction/3,
    start_transaction/2,
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
    cure:start_transaction(Clock, Properties).

start_transaction(Clock, Properties, KeepAlive) ->
    cure:start_transaction(Clock, Properties, KeepAlive).

commit_transaction(TxId) ->
    cure:commit_transaction(TxId).

abort_transaction(TxId) ->
    cure:abort_transaction(TxId).

read_objects(Objects, TxId) ->
    cure:read_objects(Objects, TxId).

read_objects(Clock, Properterties, Objects) ->
    cure:read_objects(Clock, Properterties, Objects).

read_objects(Clock, Properterties, Objects, StayAlive) ->
    cure:read_objects(Clock, Properterties, Objects, StayAlive).

update_objects(Updates, TxId) ->
    cure:update_objects(Updates, TxId).

update_objects(Clock, Properties, Updates) ->
    cure:update_objects(Clock, Properties, Updates).

update_objects(Clock, Properties, Updates, StayAlive) ->
    cure:update_objects(Clock, Properties, Updates, StayAlive).
