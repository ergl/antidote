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
    start_transaction(Clock, Properties, false).

start_transaction(_Clock, _Properties, _KeepAlive) ->
    pvc_istart_tx().

commit_transaction(TxId) ->
    cure:commit_transaction(TxId).

abort_transaction(TxId) ->
    cure:abort_transaction(TxId).

read_objects(Objects, TxId) ->
    cure:read_objects(Objects, TxId).

read_objects(_Clock, _Properterties, _Objects) ->
    %% TODO: Support static transactions
    {error, operation_not_implemented}.

read_objects(_Clock, _Properterties, _Objects, _StayAlive) ->
    %% TODO: Support static transactions
    {error, operation_not_implemented}.

-spec update_objects([{bound_object(), op_name(), op_param()}], txid()) -> ok | {error, reason()}.
update_objects(Updates, TxId) ->
    FormattedUpdates = format_update_params(Updates),
    case gen_fsm:sync_send_event(TxId#tx_id.server_pid, {update_objects, FormattedUpdates}, ?OP_TIMEOUT) of
        ok ->
            ok;

        {aborted, TxId}=Abort ->
            {error, Abort};

        {error, _R}=Err ->
            Err
    end.

update_objects(_Clock, _Properties, _Updates) ->
    %% TODO: Support static transactions
    {error, operation_not_implemented}.

update_objects(_Clock, _Properties, _Updates, _StayAlive) ->
    %% TODO: Support static transactions
    {error, operation_not_implemented}.

pvc_istart_tx() ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self() | compat_args()]),
    receive
        {ok, TxId} -> {ok, TxId};
        Err -> {error, Err}
    end.

compat_args() ->
    %% This feels hacky
    [ignore, update_clock, false].

format_update_params(Updates) ->
    lists:map(fun({{Key, Type, Bucket}, Op, Param}) ->
        {{Key, Bucket}, Type, {Op, Param}}
    end, Updates).
