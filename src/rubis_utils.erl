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

-module(rubis_utils).

-define(GROUPING_SEP, <<"+">>).

-define(KEY_GUARD(G,T,I), is_binary(G) andalso is_atom(T) andalso is_integer(I)).
-define(INDEX_GUARD(G,N), is_binary(G) andalso is_atom(N)).

%% API
-export([get_grouping/1,
         gen_key/3,
         gen_index_name/2]).

%% @doc Get the grouping information for a key
%%
%%      If a key contains no grouping information, just return the key as-is
get_grouping(Key) when is_binary(Key) ->
    hd(binary:split(Key, ?GROUPING_SEP)).

gen_key(Grouping, Table, Id) when ?KEY_GUARD(Grouping, Table, Id) ->
    gen_key(Grouping, atom_to_binary(Table, utf8), integer_to_binary(Id));

gen_key(Grouping, Table, Id) ->
    <<Grouping/binary, ?GROUPING_SEP/binary, Table/binary, Id/binary>>.

gen_index_name(Grouping, IndexName) when ?INDEX_GUARD(Grouping, IndexName) ->
    <<Grouping/binary, ?GROUPING_SEP/binary, (atom_to_binary(IndexName, utf8))/binary>>.

