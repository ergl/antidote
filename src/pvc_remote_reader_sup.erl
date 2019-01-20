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

-module(pvc_remote_reader_sup).
-behavior(supervisor).

-include("pvc.hrl").

-export([start_readers/0,
         stop_readers/0]).

-export([start_link/0,
         init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ReaderSpec = #{id => undefined,
                   start => {pvc_remote_reader, start_link, []},
                   restart => transient},
    {ok, {{simple_one_for_one, 5, 10}, [ReaderSpec]}}.

-spec start_readers() -> ok.
start_readers() ->
    start_readers(?NUM_READERS).

-spec start_readers(non_neg_integer()) -> ok.
start_readers(0) ->
    ok;

start_readers(N) ->
    case supervisor:start_child(?MODULE, [N]) of
        {ok, _Id} ->
            start_readers(N - 1);
        {error, {already_started, _}} ->
            start_readers(N - 1);
        Err ->
            Name = pvc_remote_reader:name(N),
            lager:debug("Unable to start remote reader ~p (err ~p)", [Name, Err]),
            try
                gen_server:call(Name, stop)
            catch _:_Reason ->
                ok
            end,
            start_readers(N)
    end.

-spec stop_readers() -> ok.
stop_readers() ->
    stop_readers(?NUM_READERS).

-spec stop_readers(non_neg_integer()) -> ok.
stop_readers(0) ->
    ok;

stop_readers(N) ->
    try
        gen_server:call(pvc_remote_reader:name(N), stop)
    catch _:_Reason ->
        ok
    end,
    stop_readers(N - 1).
