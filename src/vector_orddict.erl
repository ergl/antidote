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
-module(vector_orddict).
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type vector_orddict() :: {[{vectorclock(), term()}], non_neg_integer()}.
-type nonempty_vector_orddict() :: {[{vectorclock(), term()}, ...], non_neg_integer()}.

-export_type([vector_orddict/0, nonempty_vector_orddict/0]).

-export([new/0,
         get_smaller/2,
         get_smaller_from_id/3,
         insert/3,
         insert_bigger/3,
         sublist/3,
         size/1,
         to_list/1,
         from_list/1,
         first/1,
         last/1,
         filter/2]).

%% @doc The vector orddict is an ordered dictionary used to store materialized snapshots whose order
%%      is described by vectorclocks.
%%      Note that the elements are stored in a sorted list going from big to small (left to right).
-spec new() -> vector_orddict().
new() ->
    {[], 0}.

%% @doc Get the first appropiate element from the dict according to a =< ordering.
%%
%%      get_smaller(Clock, Dict) will return {{DClock, _}=Entry, IsFirst},
%%      where Entry is the most recent entry such that DClock =< Clock.
%%
%%      In addition, return IsFirst, indicating if the selected entry was the newest entry
%%      in the orddict. Returns undefined if there's no appropriate entry in the dict.
%%
-spec get_smaller(vectorclock(), vector_orddict()) -> {{vectorclock(), term()}, boolean()}
                                                    | undefined.
get_smaller(Vector, {List, _Size}) ->
    get_smaller_internal(List, Vector, true).

-spec get_smaller_internal(
    [{vectorclock(), term()}],
    vectorclock(),
    boolean()
) -> {{vectorclock(), term()}, boolean()} | undefined.

get_smaller_internal([], _, _) ->
    undefined;

get_smaller_internal([{FirstClock, _}=Version | Rest], Vector, IsFirst) ->
    case vectorclock:le(FirstClock, Vector) of
        true ->
            {Version, IsFirst};
        false ->
            get_smaller_internal(Rest, Vector, false)
    end.

%% @doc Same as get_smaller, according to a =< ordering on the id-th entry of each vectorclock.
%%
%%      get_smaller_from_id(Id, Time, Dict) will return {Clock, _}=Entry,
%%      where Entry is the most recent entry such that Clock[Id] =< Time.
%%
%%      Returns undefined if there's no appropriate entry in the dict.
%%
-spec get_smaller_from_id(term(), clock_time(), vector_orddict()) -> {vectorclock(), term()}
                                                                   | undefined.

get_smaller_from_id(_Id, _Time, {_List, Size}) when Size == 0 ->
    undefined;

get_smaller_from_id(Id, Time, {List, _Size}) ->
    get_smaller_from_id_internal(Id, Time, List).

-spec get_smaller_from_id_internal(
    term(),
    clock_time(),
    [{vectorclock(), term()}]
) -> {vectorclock(), term()} | undefined.

get_smaller_from_id_internal(_Id, _Time, []) ->
    undefined;

get_smaller_from_id_internal(Id, Time, [{Clock, Val}|Rest]) ->
    ValTime = vectorclock:get_clock_of_dc(Id, Clock),
    case ValTime =< Time of
        true ->
            {Clock, Val};
        false ->
            get_smaller_from_id_internal(Id, Time, Rest)
    end.

%% @doc Insert a snapshot in the vector, at the appropiate position
%%
%%      Snapshots are ordered from more recent to less recent, left to right
%%
-spec insert(vectorclock(), term(), vector_orddict()) -> vector_orddict().
insert(Vector, Val, {List, Size}) ->
    NewSize = Size + 1,
    {insert_internal(Vector, Val, List, []), NewSize}.

-spec insert_internal(
    vectorclock(),
    term(),
    [{vectorclock(), term()}],
    [{vectorclock(), term()}]
) -> [{vectorclock(), term()}].

insert_internal(Vector, Val, [], Acc) ->
    lists:reverse([{Vector, Val} | Acc]);

insert_internal(Vector, Val, All, Acc) ->
    [{FirstClock, _} = Version | Rest] = All,

    case vectorclock:all_dots_greater(Vector, FirstClock) of
        true ->
            lists:reverse(Acc, [{Vector, Val} | All]);

        false ->
            insert_internal(Vector, Val, Rest, [Version | Acc])
    end.

%% @doc Insert a snapshot in the vector, if it's the biggest of all, do nothing otherwise
%%
%%      insert_bigger(Clock, _V, Dict) will insert {Clock, _V} if Clock > max(Dict),
%%      or if Dict is empty.
%%
-spec insert_bigger(vectorclock(), term(), vector_orddict()) -> nonempty_vector_orddict().
insert_bigger(Vector, Val, {List, Size}) ->
    insert_bigger_internal(Vector, Val, List, Size).

-spec insert_bigger_internal(vectorclock(), term(), [{vectorclock(), term()}], non_neg_integer()) -> nonempty_vector_orddict().
insert_bigger_internal(Vector, Val, [], 0) ->
    {[{Vector, Val}], 1};

insert_bigger_internal(Vector, Val, All, Size) ->
    [{FirstClock, _} | _] = All,
    case not vectorclock:le(Vector, FirstClock) of
        true ->
            {[{Vector, Val} | All], Size + 1};

        false ->
            {All, Size}
    end.

-spec sublist(vector_orddict(), non_neg_integer(), non_neg_integer()) -> vector_orddict().
sublist({List, _Size}, Start, Len) ->
    Res = lists:sublist(List, Start, Len),
    {Res, length(Res)}.

-spec size(vector_orddict()) -> non_neg_integer().
size({_List, Size}) ->
    Size.

-spec to_list(vector_orddict()) -> [{vectorclock(), term()}].
to_list({List, _Size}) ->
    List.

-spec from_list([{vectorclock(), term()}]) -> vector_orddict().
from_list(List) ->
    {List, length(List)}.

-spec first(vector_orddict()) -> {vectorclock(), term()}.
first({[First|_Rest], _Size}) ->
    First.

-spec last(vector_orddict()) -> {vectorclock(), term()}.
last({List, _Size}) ->
    lists:last(List).

-spec filter(fun((term()) -> boolean()), vector_orddict()) -> vector_orddict().
filter(Fun, {List, _Size}) ->
    Result = lists:filter(Fun, List),
    {Result, length(Result)}.

-ifdef(TEST).

vector_oddict_get_smaller_from_id_test() ->
  %% Fill up the vector
  Vdict0 = vector_orddict:new(),
  CT1 = vectorclock:from_list([{dc1, 4}, {dc2, 4}]),
  Vdict1 = vector_orddict:insert(CT1, 1, Vdict0),
  CT2 = vectorclock:from_list([{dc1, 8}, {dc2, 8}]),
  Vdict2 = vector_orddict:insert(CT2, 2, Vdict1),
  CT3 = vectorclock:from_list([{dc1, 1}, {dc2, 10}]),
  Vdict3 = vector_orddict:insert(CT3, 3, Vdict2),

  %% Check you get the correct smaller snapshot
  ?assertEqual(undefined, vector_orddict:get_smaller_from_id(dc1, 0, Vdict0)),
  ?assertEqual(undefined, vector_orddict:get_smaller_from_id(dc1, 0, Vdict3)),
  ?assertEqual({CT3, 3}, vector_orddict:get_smaller_from_id(dc1, 1, Vdict3)),
  ?assertEqual({CT2, 2}, vector_orddict:get_smaller_from_id(dc2, 9, Vdict3)).


vector_orddict_get_smaller_test() ->
  %% Fill up the vector
  Vdict0 = vector_orddict:new(),
  CT1 = vectorclock:from_list([{dc1, 4}, {dc2, 4}]),
  Vdict1 = vector_orddict:insert(CT1, 1, Vdict0),
  CT2 = vectorclock:from_list([{dc1, 8}, {dc2, 8}]),
  Vdict2 = vector_orddict:insert(CT2, 2, Vdict1),
  CT3 = vectorclock:from_list([{dc1, 1}, {dc2, 10}]),
  Vdict3 = vector_orddict:insert(CT3, 3, Vdict2),

  %% Check you get the correct smaller snapshot
  ?assertEqual(undefined, vector_orddict:get_smaller(vectorclock:from_list([{dc1, 0}, {dc2, 0}]), Vdict3)),
  ?assertEqual(undefined, vector_orddict:get_smaller(vectorclock:from_list([{dc1, 1}, {dc2, 6}]), Vdict3)),
  ?assertEqual({{CT1, 1}, false}, vector_orddict:get_smaller(vectorclock:from_list([{dc1, 5}, {dc2, 5}]), Vdict3)),
  ?assertEqual({{CT2, 2}, true}, vector_orddict:get_smaller(vectorclock:from_list([{dc1, 9}, {dc2, 9}]), Vdict3)),
  ?assertEqual({{CT3, 3}, false}, vector_orddict:get_smaller(vectorclock:from_list([{dc1, 3}, {dc2, 11}]), Vdict3)).


vector_orddict_insert_bigger_test() ->
  Vdict0 = vector_orddict:new(),
  %% Insert to empty dict
  CT1 = vectorclock:from_list([{dc1, 4}, {dc2, 4}]),
  Vdict1 = vector_orddict:insert_bigger(CT1, 1, Vdict0),
  ?assertEqual(1, vector_orddict:size(Vdict1)),
  %% Should not insert because smaller
  CT2 = vectorclock:from_list([{dc1, 3}, {dc2, 3}]),
  Vdict2 = vector_orddict:insert_bigger(CT2, 2, Vdict1),
  ?assertEqual(1, vector_orddict:size(Vdict2)),
  %% Should insert because bigger
  CT3 = vectorclock:from_list([{dc1, 6}, {dc2, 10}]),
  Vdict3 = vector_orddict:insert_bigger(CT3, 3, Vdict2),
  ?assertEqual(2, vector_orddict:size(Vdict3)).


vector_orddict_filter_test() ->
    VDict = vector_orddict:from_list([
        {vectorclock:from_list([{dc1, 4}, {dc2, 4}]), snapshot_1},
        {vectorclock:from_list([{dc1, 0}, {dc2, 3}]), snapshot_2},
        {vectorclock:new(), snapshot_3}
    ]),

    ?assertEqual(3, vector_orddict:size(VDict)),

    Result = vector_orddict:filter(fun({Vector, _}) ->
        vectorclock:gt(Vector, vectorclock:new())
    end, VDict),

    ?assertEqual(2, vector_orddict:size(Result)),
    Filtered = [
        {vectorclock:from_list([{dc1, 4}, {dc2, 4}]), snapshot_1},
        {vectorclock:from_list([{dc1, 0}, {dc2, 3}]), snapshot_2}
    ],
    ?assertEqual(Filtered, vector_orddict:to_list(Result)).

-endif.
