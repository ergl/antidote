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

%% Transaction assembler reads a stream of log operations and produces complete transactions.

-module(log_txn_assembler).
-include("antidote.hrl").
-include("inter_dc_repl.hrl").

%% API
-export([new_buffer/0,
         process/2,
         process_all/2]).

%% State
-opaque buffer() :: dict:dict(txid(), [#log_record{}]).

-export_type([buffer/0]).

%%%% API --------------------------------------------------------------------+

-spec new_buffer() -> buffer().
new_buffer() ->
    dict:new().

-spec process(#log_record{}, buffer()) -> {{ok, [#log_record{}]} | none, buffer()}.
process(LogRecord, Buffer) ->
    Payload = LogRecord#log_record.log_operation,
    TxId = Payload#log_operation.tx_id,
    NewTxnBuf = find_or_default(TxId, Buffer, []) ++ [LogRecord],
    case Payload#log_operation.op_type of
        commit ->
            {{ok, NewTxnBuf}, dict:erase(TxId, Buffer)};

        abort ->
            {none, dict:erase(TxId, Buffer)};

        _ ->
            {none, dict:store(TxId, NewTxnBuf, Buffer)}
  end.

-spec process_all([#log_record{}], buffer()) -> {[[#log_record{}]], buffer()}.
process_all(LogRecords, Buffer) ->
    process_all(LogRecords, [], Buffer).

-spec process_all([#log_record{}], [[#log_record{}]], buffer()) -> {[[#log_record{}]], buffer()}.
process_all([], Acc, Buffer) ->
    {Acc, Buffer};

process_all([H|T], Acc, Buffer) ->
    {Result, NewBuffer} = process(H, Buffer),
    NewAcc = case Result of
        none ->
            Acc;
        {ok, Txn} ->
            Acc ++ [Txn]
    end,
    process_all(T, NewAcc, NewBuffer).

%%%% Methods ----------------------------------------------------------------+

-spec find_or_default(#tx_id{}, dict:dict(), any()) -> any().
find_or_default(Key, Dict, Default) ->
    case dict:find(Key, Dict) of
        {ok, Val} ->
            Val;
        _ ->
            Default
    end.
