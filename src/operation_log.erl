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

-module(operation_log).

-include("antidote.hrl").

%% Compat layer
-export([request_op_id/3,
         get/5,
         get_all/4]).


request_op_id(_Arg0, _Arg1, _Arg2) ->
    erlang:error(not_implemented).

get(_Arg0, _Arg1, _Arg2, _Arg3, _Arg4) ->
    erlang:error(not_implemented).

get_all(_Arg0, _Arg1, _Arg2, _Arg3) ->
    erlang:error(not_implemented).
