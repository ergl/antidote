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

-module(clocksi_operation_log).

-include("antidote.hrl").

-behavior(gen_server).

%% Compat layer
-export([get/5,
         get_all/4]).

%% Clock-SI logic wrappers.
-export([append/3
%%    , async_append/3
%%    , append_commit/3
%%    , append_all/3
]).

%% gen_server callbacks
-export([start_link/0,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {
    bucket_table :: cache_id()
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get(_Arg0, _Arg1, _Arg2, _Arg3, _Arg4) ->
    erlang:error(not_implemented).

get_all(_Arg0, _Arg1, _Arg2, _Arg3) ->
    erlang:error(not_implemented).

%% Look at logging_vnode:append/3
%% Most of the insert logic is the same, but it does some checks depending
%% on the kind of operation it receives.
%%
%% If the operation is an update, it stores some op id at the bucket level.
%% Create a new log record (as in #log_record{}). After it has stored it
%% on the log, it sends it through inter_dc_log_sender:send/2
append(IndexNode, LogId, LogOperation) ->
    gen_server:call(?MODULE, {append, IndexNode, LogId, LogOperation}, infinity).

init(_Args) ->
    BucketTable = ets:new(bucket_op_id_table, [set]),
    {ok, #state{bucket_table = BucketTable}}.

handle_call(_Request, _Sender, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
