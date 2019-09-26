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

-module(coord_req_promise).

-record(promise, {
    reply_to :: pid(),
    context :: term()
}).


-type promise() :: #promise{}.

-export([new/2,
         resolve/2]).


-spec new(pid(), term()) -> promise().
new(From, Context) ->
    #promise{reply_to=From, context=Context}.

-spec resolve(term(), promise()) -> ok.
resolve(Reply, #promise{reply_to=To, context=Context}) ->
    To ! {promise_resolve, Reply, Context},
    ok.
