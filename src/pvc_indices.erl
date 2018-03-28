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

-module(pvc_indices).

-include("antidote.hrl").

-define(CLAIMED, claimed).
-define(INDEX_SEP, <<"$">>/binary).
-define(UINDEX_SEP, <<"%">>/binary).

-opaque range() :: {binary(), non_neg_integer()}.

-export_type([range/0]).

%% API
-export([u_index/4,
         read_u_index/3,
         index/4,
         read_index/2,
         read_index/3]).

-export([in_range/2]).

-spec u_index(binary(), binary(), binary(), txid()) -> ok.
u_index(IndexName, IndexValue, RefKey, TxId) ->
    Key = make_u_index_key(IndexName, IndexValue),
    pvc:update_keys({Key, RefKey}, TxId).

-spec read_u_index(binary(), binary(), txid()) -> {ok, list()} | {error, reason()}.
read_u_index(IndexName, IndexValue, TxId) ->
    Key = make_u_index_key(IndexName, IndexValue),
    pvc:read_keys(Key, TxId).

-spec index(binary(), binary(), binary(), txid()) -> ok.
index(IndexName, IndexValue, RefKey, TxId) ->
    MainIndexKey = make_root_index_key(IndexName),
    RootKey = make_root_index_key(IndexName, IndexValue),
    IndexKey = make_index_key(IndexName, IndexValue, RefKey),
    MainUpdate = case claimed_index(MainIndexKey, TxId) of
        false ->
            [{MainIndexKey, ?CLAIMED}];
        true ->
            []
    end,
    Updates = case claimed_index(RootKey, TxId) of
        false ->
            [{RootKey, ?CLAIMED}, {IndexKey, RefKey} | MainUpdate];

        true ->
            [{IndexKey, RefKey} | MainUpdate]
    end,
    update_indices(Updates, TxId).

-spec read_index(binary(), txid()) -> {ok, list()} | {error, reason()}.
read_index(IndexName, TxId) ->
    MainIndexKey = make_root_index_key(IndexName),
    case claimed_index(MainIndexKey, TxId) of
        false ->
            {ok, []};
        true ->
            {ok, Range} = read_index_range(MainIndexKey, TxId),
            Filtered = lists:filter(fun(Key) ->
                length(binary:split(Key, <<"$">>, [global])) > 2
            end, Range),
            case Filtered of
                [] ->
                    {ok, []};
                _ ->
                    pvc:read_keys(Filtered, TxId)
            end
    end.

-spec read_index(binary(), binary(), txid()) -> {ok, list()} | {error, reason()}.
read_index(IndexName, IndexValue, TxId) ->
    RootKey = make_root_index_key(IndexName, IndexValue),
    case claimed_index(RootKey, TxId) of
        false ->
            {ok, []};
        true ->
            {ok, Range} = read_index_range(RootKey, TxId),
            case Range of
                [] ->
                    {ok, []};
                _ ->
                    pvc:read_keys(Range, TxId)
            end
    end.

%% FIXME(borja): Might yield false positives
%%
%% For example,
%% <<"AZ--Phoenix+bids_bidder_id$AZ--Phoenix+users1">>
%% and
%% <<"AZ--Phoenix+bids_bidder_id$AZ--Phoenix+users10">>
%% are not subkeys, but this will return true,
%% as the prefixes are the same up until `...users1`
-spec in_range(binary(), range()) -> boolean().
in_range(Key, {_, Len}) when byte_size(Key) < Len ->
    false;

in_range(Key, {Prefix, Len}) ->
    Len =:= binary:longest_common_prefix([Prefix, Key]).

%% Util functions

claimed_index(RootKey, TxId) ->
    {ok, [RootVal]} = pvc:read_keys(RootKey, TxId),
    RootVal =:= ?CLAIMED.

%% TODO(borja): Handle non-binary data
make_u_index_key(IndexName, IndexValue) ->
    <<IndexName/binary, ?UINDEX_SEP, IndexValue/binary>>.

%% TODO(borja): Handle non-binary data
make_root_index_key(IndexName) ->
    IndexName.

make_root_index_key(IndexName, IndexValue) ->
    <<IndexName/binary, ?INDEX_SEP, IndexValue/binary>>.

%% TODO(borja): Handle non-binary data
make_index_key(IndexName, IndexValue, RefKey) ->
    <<IndexName/binary, ?INDEX_SEP, IndexValue/binary, ?INDEX_SEP, RefKey/binary>>.

update_indices(Updates, TxId = #tx_id{server_pid = Pid}) ->
    ok = pvc:update_keys(Updates, TxId),
    gen_fsm:sync_send_event(Pid, {pvc_index, Updates}, ?OP_TIMEOUT).

read_index_range(RootKey, #tx_id{server_pid = Pid}) ->
    Range = make_range(RootKey),
    gen_fsm:sync_send_event(Pid, {pvc_scan_range, {RootKey, Range}}).

-spec make_range(binary()) -> range().
make_range(Key) ->
    PrefixLen = byte_size(Key),
    {Key, PrefixLen}.
