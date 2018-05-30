%%%% -------------------------------------------------------------------
%%%%
%%%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%%%
%%%% This file is provided to you under the Apache License,
%%%% Version 2.0 (the "License"); you may not use this file
%%%% except in compliance with the License.  You may obtain
%%%% a copy of the License at
%%%%
%%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%%
%%%% Unless required by applicable law or agreed to in writing,
%%%% software distributed under the License is distributed on an
%%%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%%%% KIND, either express or implied.  See the License for the
%%%% specific language governing permissions and limitations
%%%% under the License.
%%%%
%%%% -------------------------------------------------------------------

-module(rubis).

-export([process_request/2]).

process_request('Ping', _) ->
    {ok, TxId} = antidote:start_transaction(ignore, []),
    Commit = antidote:commit_transaction(TxId),
    case Commit of
        {ok, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end;

process_request('Load', #{num_keys := N, bin_size := Size}) ->
    Val = crypto:strong_rand_bytes(Size),
    {Keys, Updates} = lists:foldl(fun(K, {KeyAcc, UpdateAcc}) ->
        Gen = integer_to_binary(K, 36),
        {[Gen | KeyAcc], [{Gen, Val} | UpdateAcc]}
    end, {[],[]}, lists:seq(1, N)),

    {ok, TxId} = antidote:start_transaction(ignore, []),
    case read_keys_batch(Keys, TxId) of
        {error, _}=ReadError ->
            ReadError;
        {ok, _} ->
            ok = update_keys(Updates, TxId),
            Commit = antidote:commit_transaction(TxId),
            case Commit of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end;

process_request('ReadOnlyTx', #{keys := Keys}) ->
    {ok, TxId} = antidote:start_transaction(ignore, []),
    case read_keys_sequential(Keys, TxId) of
        {error, _}=ReadError ->
            ReadError;
        ok ->
            Commit = antidote:commit_transaction(TxId),
            case Commit of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end;

process_request('ReadWriteTx', #{read_keys := Keys, ops := OpList}) ->
    Updates = lists:map(fun(#{key := K, value := V}) -> {K, V} end, OpList),

    {ok, TxId} = antidote:start_transaction(ignore, []),
    case read_keys_sequential(Keys, TxId) of
        {error, _}=ReadError ->
            ReadError;
        ok ->
            ok = update_keys(Updates, TxId),
            Commit = antidote:commit_transaction(TxId),
            case Commit of
                {ok, _} ->
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

read_keys_batch(Keys, TxId) ->
    Objs = lists:map(fun(K) ->
        {K, antidote_crdt_register_lww, my_bucket}
    end, Keys),
    antidote:read_objects(Objs, TxId).

read_keys_sequential(Keys, TxId) ->
    Objs = lists:map(fun(K) ->
        {K, antidote_crdt_register_lww, my_bucket}
    end, Keys),
    read_keys_sequential_int(Objs, TxId).

read_keys_sequential_int([], _TxId) ->
    ok;

read_keys_sequential_int([Obj | Objs], TxId) ->
    case antidote:read_objects([Obj], TxId) of
        {error, _}=ReadError ->
            ReadError;
        {ok, _} ->
            read_keys_sequential_int(Objs, TxId)
    end.

update_keys(Updates, TxId) ->
    Ops = lists:map(fun({K, V}) ->
        {{K, antidote_crdt_register_lww, my_bucket}, assign, V}
    end, Updates),
    antidote:update_objects(Ops, TxId).
