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

-module(log_compat).

-include("antidote.hrl").

%% API
-export([set_sync_log/1,
         read/2,
         append/3,
         asyn_append/4,
         append_commit/3,
         request_op_id/3,
         append_group/4,
         get/5,
         get_all/4]).

%% Compat layer
-export([get_master_node/0,
         get_vnode_module/0,
         get_logging_module/0]).

get_logging_module() ->
    case application:get_env(antidote, log_version) of
        {ok, basic} ->
            operation_log;
        {ok, legacy} ->
            logging_vnode
    end.

get_vnode_module() ->
    case application:get_env(antidote, log_version) of
        {ok, basic} ->
            basic_logging_vnode;
        {ok, legacy} ->
            logging_vnode
    end.

get_master_node() ->
    case application:get_env(antidote, log_version) of
        {ok, basic} ->
            basic_logging_vnode_master;
        {ok, legacy} ->
            logging_vnode_master
    end.

%% VNode, generic
set_sync_log(Value) ->
    Module = get_vnode_module(),
    Module:set_sync_log(Value).

%% VNode, generic
read(Node, Log) ->
    Module = get_vnode_module(),
    Module:read(Node, Log).

%% VNode, generic
append(Node, Log, Op) ->
    Module = get_vnode_module(),
    Module:append(Node, Log, Op).

%% VNode, generic, different names
asyn_append(IndexNode, Log, LogOperation, ReplyTo) ->
    case get_vnode_module() of
        basic_logging_vnode ->
            basic_logging_vnode:async_append(IndexNode, Log, LogOperation, ReplyTo);
        Module ->
            Module:asyn_append(IndexNode, Log, LogOperation, ReplyTo)
    end.

%% VNode, generic
append_commit(Node, LogId, Op) ->
    Module = get_vnode_module(),
    Module:append_commit(Node, LogId, Op).

%% Clock-SI specific
request_op_id(IndexNode, DCID, Partition) ->
    Module = get_logging_module(),
    Module:request_op_id(IndexNode, DCID, Partition).

%% VNode, generic
append_group(Node, LogId, Ops, _IgnoredInClockSI=false) ->
    case get_vnode_module() of
        basic_logging_vnode ->
            basic_logging_vnode:append_all(Node, LogId, Ops);
        Module ->
            Module:append_group(Node, LogId, Ops, false)
    end.

%% Clock-SI specific
-spec get(index_node(), key(), vectorclock(), term(), key()) -> #snapshot_get_response{} | {error, term()}.
get(IndexNode, LogId, MinSnapshotTime, Type, Key) ->
    Module = get_logging_module(),
    Module:get(IndexNode, LogId, MinSnapshotTime, Type, Key).

%% Clock-SI specific
get_all(IndexNode, LogId, Continuation, PrevOps) ->
    Module = get_logging_module(),
    Module:get_all(IndexNode, LogId, Continuation, PrevOps).
