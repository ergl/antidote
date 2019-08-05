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
-module(rubis_keygen_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%% API
-export([next_id/2]).

%% riak_core_vnode callbacks
-export([start_vnode/1,
         init/1,
         handle_command/3,
         handle_coverage/4,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_command/3,
         handle_handoff_data/2,
         encode_handoff_item/2,
         is_empty/1,
         terminate/2,
         handle_exit/3,
         delete/1]).

-ignore_xref([start_vnode/1]).

-define(TABLE_NAME, rubis_key_table).

-record(state, {
    partition :: partition_id(),
    key_table :: cache_id()
}).

-spec next_id(index_node(), any()) -> non_neg_integer().
next_id(Node, RubisTable) ->
    TableName = table_name(Node, ?TABLE_NAME),
    case ets:info(TableName) of
        undefined ->
            riak_core_vnode_master:sync_command(Node, {faa, RubisTable}, rubis_keygen_vnode_master);
        _ ->
            faa(RubisTable, TableName)
    end.

table_name({Partition, _}, Name) ->
    table_name(Partition, Name);

table_name(Partition, Name) ->
    try
        list_to_existing_atom(atom_to_list(Name) ++ "-" ++ integer_to_list(Partition))
    catch _:_ ->
        list_to_atom(atom_to_list(Name) ++ "-" ++ integer_to_list(Partition))
    end.

-spec faa(atom(), ets:tid()) -> non_neg_integer().
faa(RubisTable, KeyTable) ->
    %% Update the id for the given table
    %% If no such `Table` key exists, then insert {Table, 0}
    %% on the KeyTable, and the do the update_counter
    ets:update_counter(KeyTable, RubisTable, 1, {RubisTable, 0}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    KeyTable = ets:new(table_name(Partition, ?TABLE_NAME), [set, public, named_table, {write_concurrency, true}]),
    {ok, #state{key_table = KeyTable,
                partition = Partition}}.

handle_command({faa, Table}, _Sender, State = #state{key_table = KeyTable}) ->
    {reply, faa(Table, KeyTable), State};

handle_command(Message, _Sender, State) ->
    lager:info("unhandled_command ~p", [Message]),
    {noreply, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handoff_starting(_, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_, State) ->
    {ok, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_data(_Arg0, _Arg1) ->
    erlang:error(not_implemented).

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

delete(State) ->
    {ok, State}.
