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
         read_keys/2,
         update_keys/2,
         commit_transaction/1]).

%% API
-export([
    start_transaction/3,
    start_transaction/2,
    abort_transaction/1,
    read_objects/2,
    read_objects/3,
    read_objects/4,
    update_objects/2,
    update_objects/3,
    update_objects/4
]).

%% PVC-Only API

start_transaction() ->
    pvc_istart_tx().

commit_transaction(TxId) ->
    CommitRes = gen_fsm:sync_send_event(TxId#tx_id.server_pid, {prepare, pvc_commit}, ?OP_TIMEOUT),
    case CommitRes of
        ok ->
%%            lager:info("{~p} PVC commit", [erlang:phash2(TxId)]),
            {ok, []};
        Res -> Res
    end.

read_keys(Keys, #tx_id{server_pid = Pid}) when is_list(Keys) ->
    CompatKeys = lists:map(fun(K) ->
        {K, antidote_crdt_lwwreg}
    end, Keys),
    gen_fsm:sync_send_event(Pid, {read_objects, CompatKeys}, ?OP_TIMEOUT);

read_keys(Key, TxId) ->
    read_keys([Key], TxId).

update_keys(UpdateOps, TxId = #tx_id{server_pid = Pid}) when is_list(UpdateOps) ->
    CompatOps = lists:map(fun({K, V}) ->
        {K, antidote_crdt_lwwreg, {assign, V}}
    end, UpdateOps),
    Resp = gen_fsm:sync_send_event(Pid, {update_objects, CompatOps}, ?OP_TIMEOUT),
    case Resp of
        ok ->
            ok;

        {aborted, TxId}=Abort ->
            {error, Abort};

        {error, _R}=Err ->
            Err
    end;

update_keys(UpdateOp, TxId) ->
    update_keys([UpdateOp], TxId).

%% Antidote-Compatible API

start_transaction(Clock, Properties) ->
    start_transaction(Clock, Properties, false).

start_transaction(_Clock, _Properties, _KeepAlive) ->
    pvc_istart_tx().

abort_transaction(TxId) ->
    cure:abort_transaction(TxId).

read_objects(Objects, TxId) ->
    case valid_objects(Objects) of
        false ->
            {error, type_not_supported};
        true ->
            FormattedObjects = format_read_params(Objects),
            Resp = gen_fsm:sync_send_event(TxId#tx_id.server_pid, {read_objects, FormattedObjects}, ?OP_TIMEOUT),
            case Resp of
                {ok, Res} ->
                    {ok, Res};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% Static Tx
read_objects(_Clock, _Properterties, _Objects) ->
    {error, operation_not_implemented}.

%% Static Tx
read_objects(_Clock, _Properterties, _Objects, _StayAlive) ->
    {error, operation_not_implemented}.

-spec update_objects([{bound_object(), op_name(), op_param()}], txid()) -> ok | {error, reason()}.
update_objects(Updates, TxId) ->
    case valid_updates(Updates) of
        false ->
            {error, type_not_supported};
        true ->
            FormattedUpdates = format_update_params(Updates),
            Resp = gen_fsm:sync_send_event(TxId#tx_id.server_pid, {update_objects, FormattedUpdates}, ?OP_TIMEOUT),
            case Resp of
                ok ->
                    ok;

                {aborted, TxId}=Abort ->
                    {error, Abort};

                {error, _R}=Err ->
                    Err
            end
    end.

%% Static Tx
update_objects(_Clock, _Properties, _Updates) ->
    {error, operation_not_implemented}.

%% Static Tx
update_objects(_Clock, _Properties, _Updates, _StayAlive) ->
    {error, operation_not_implemented}.

pvc_istart_tx() ->
    {ok, _} = clocksi_interactive_tx_coord_sup:start_fsm([self() | compat_args()]),
    receive
        {ok, TxId} ->
%%            lager:info("{~p} PVC start transaction", [erlang:phash2(TxId)]),
            {ok, TxId};
        Err -> {error, Err}
    end.

compat_args() ->
    %% This feels hacky
    [ignore, update_clock, false].

format_read_params(ReadObjects) ->
    lists:map(fun({Key, Type, Bucket}) ->
        {format_key(Key, Bucket), Type}
    end, ReadObjects).

format_update_params(Updates) ->
    lists:map(fun({{Key, Type, Bucket}, Op, Param}) ->
        {format_key(Key, Bucket), Type, {Op, Param}}
    end, Updates).

format_key(Key, Bucket) ->
    {Key, Bucket}.

valid_updates(Updates) ->
    lists:all(fun valid_update/1, Updates).

valid_update({Obj, _, _}) ->
    valid_object(Obj).

valid_objects(Objects) ->
    lists:all(fun valid_object/1, Objects).

valid_object({_, antidote_crdt_lwwreg, _}) ->
    true;

valid_object(_) ->
    false.
