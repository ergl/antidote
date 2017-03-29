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

-module(antidote_config).
-behaviour(gen_server).

-export([start_link/0,
         get/1,
         get/2,
         put/2]).

%% gen_server callbask
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-type state() :: dict:dict(atom(), term()).
-type value() :: atom().

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get(atom()) -> {ok, value() | undefined}.
get(Key) ->
    get(Key, undefined).

-ifdef(TEST).

-spec get(atom(), value()) -> {ok, value()}.
get(Key, Default) ->
    EnvKey = string:to_upper(atom_to_list(Key)),
    RawValue = os:getenv(EnvKey, Default),
    Value = case is_atom(RawValue) of
        true -> RawValue;
        false -> list_to_atom(RawValue)
    end,
    {ok, Value}.

-else.

-spec get(atom(), value()) -> {ok, value()}.
get(Key, Default) ->
    gen_server:call(?MODULE, {get, Key, Default}, infinity).

-endif.

-spec put(atom(), value()) -> ok.
put(Key, Value) ->
    gen_server:call(?MODULE, {put, Key, Value}, infinity).

-spec init([term()]) -> {ok, state()}.
init([]) ->
    {ok, dict:new()}.

%% @doc Fetch a value from the configuration.
%%
%% If no value is found, get it from the application
%% configuration, and cache it for future calls.
handle_call({get, Key, Default}, _From, State) ->
    {Val, NewState} = case dict:find(Key, State) of
        {ok, Value} -> {Value, State};
        error ->
            ExternalValue = case application:get_env(antidote, Key) of
                {ok, Value} -> Value;
                undefined -> Default
            end,
            {ExternalValue, dict:store(Key, ExternalValue, State)}
    end,
    {reply, {ok, Val}, NewState};

handle_call({put, Key, Value}, _From, State) ->
    {reply, ok, dict:store(Key, Value, State)}.

handle_cast(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

handle_info(Msg, State) ->
    lager:warning("Unhandled messages: ~p", [Msg]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
