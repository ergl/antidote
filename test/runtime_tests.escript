#!/usr/bin/env escript
%%! -smp enable -name runtimetests@127.0.0.1 -setcookie antidote

-include_lib("eunit/include/eunit.hrl").

-define (KEY(Key, Bucket), {Key, antidote_crdt_lwwreg, Bucket}).
-define (KEY(Key), ?KEY(Key, default_bucket)).

-define (TESTS, [
  fun(N) -> connect_test(N) end,
  fun(N) -> write_write_check(N) end,
  fun(N) -> stress_partition(N) end,
  fun(N) -> read_log_test(N) end,
  fun(N) -> same_partition_test(N) end,
  fun(N) -> clog_test(N) end
]).

main(_) ->
  lists:foreach(fun(El) ->
    ok = El('antidote@127.0.0.1')
  end, ?TESTS).

connect_test(Node) ->
  Key = ?KEY(connect_key),
  Val = success,

  [_, {CT, Res}] = execute_blocking_sequential([
    blocking_read_write_tx(Node, {Key, Val}),
    blocking_read_only_tx(Node, Key)
  ]),

  ?assertMatch({ok, []}, CT),
  ?assertEqual({ok, [Val]}, Res),

  ok.

write_write_check(Node) ->
  ConflictKey = ?KEY(conflict),
  Res = execute_parallel([
    read_write_tx(Node, {ConflictKey, 1}),
    read_write_tx(Node, {ConflictKey, 2})
  ]),

  CTRes = lists:map(fun({CT, _}) -> CT end, Res),

  ?assertNotMatch([{ok, _}, {ok, _}], CTRes),
  ?assert(lists:any(fun({error, {aborted, _}}) -> true; (_) -> false end, CTRes)).

stress_partition(Node) ->
  Key = ?KEY(stress_key),
  Updates = 20,

  Txs = lists:map(
    fun(N) -> read_write_tx(Node, {Key, N}) end,
    lists:seq(0, Updates)
  ),

  Res = execute_sequential(Txs),
  ?assert(lists:all(fun({{ok, _}, _}) -> true; (_) -> false end, Res)),

  {{Status, _}, {_, Payload}} = execute_sequential(read_only_tx(Node, Key)),

  ?assertMatch(ok, Status),
  ?assertEqual([20], Payload).

read_log_test(Node) ->
  Key = ?KEY(read_log),
  BaseValue = initial_value,

  Updates = 15,
  UpdateTxs = lists:map(
    fun(N) -> blocking_read_write_tx(Node, {Key, N}) end,
    lists:seq(0, Updates)
  ),

  %% First, set the key to some initial value we can validate later
  _ = execute_blocking_sequential([blocking_read_write_tx(Node, {Key, BaseValue})]),

  %% Validate that the base value went through
  {{Status, _}, {_, Payload}} = execute_blocking_sequential(blocking_read_only_tx(Node, Key)),
  ?assertMatch(ok, Status),
  ?assertEqual([BaseValue], Payload),

  %% Now start a long-running transaction
  {ok, Tx} = start_transaction(Node),

  %% Pick up a dependency on the previous transaction
  {ok, Read1} = read_objects(Node, Tx, Key),
  ?assertEqual([BaseValue], Read1),

  timer:sleep(100),

  %% Now stress the partition by forcing it to garbage collect
  Res = execute_blocking_sequential(UpdateTxs),
  ?assert(lists:all(fun({{ok, _}, _}) -> true; (_) -> false end, Res)),

  %% Now read again. Because we picked up an empty clock,
  %% the snapshot should no longer be in memory, and has to
  %% be retrieved from the event log.
  {ok, SecondRead} = read_objects(Node, Tx, Key),

  ?assertEqual([BaseValue], SecondRead),

  {ok, []} = commit_transaction(Node, Tx),
  ok.

same_partition_test(Node) ->
  KeyA = ?KEY(key_a),
  ValueA = value_a,

  KeyB = ?KEY(key_b),
  ValueB = value_b,

  _ = execute_blocking_sequential(
    blocking_read_write_tx(Node, [{KeyA, ValueA}, {KeyB, ValueB}])
  ),

  {CT, Read} = execute_blocking_sequential(blocking_read_only_tx(Node, [KeyA, KeyB])),
  ?assertMatch({ok, []}, CT),

  {Status, [ReadA, ReadB]} = Read,
  ?assertMatch(ok, Status),
  ?assertEqual(ValueA, ReadA),
  ?assertEqual(ValueB, ReadB),
  ok.

clog_test(Node) ->
  KeyA = ?KEY(partition_a),
  KeyB = ?KEY(partition_b),
  KeyC = ?KEY(partition_c),

  %% Set the clock to <2, 1, 1>
  _ = execute_blocking_sequential([
    %% <1, 0, 0> || <0, 0, 0> || <0, 0, 0>
    blocking_read_write_tx(Node, {KeyA, <<"Aa">>}),
    %% <2, 1, 1> || <2, 1, 1> || <2, 1, 1>
    blocking_read_write_tx(Node, [{KeyA, <<"Ab">>}, {KeyB, <<"Ba">>}, {KeyC, <<"Ca">>}])
  ]),

  %% Now start a long-running transaction
  {ok, Tx} = start_transaction(Node),

  %% Pick up dependency clock <_, 1, 1>
  {ok, ReadBC} = read_objects(Node, Tx, [KeyB, KeyC]),
  ?assertEqual([<<"Ba">>, <<"Ca">>], ReadBC),

  %% Now, interleave a bunch of transactions on both A, B and C
  %% Final MRVC should be <5, 7, 4>
  _ = execute_blocking_sequential([
    %% <3, 1, 1> || <2, 1, 1> || <2, 1, 1>
    %% We should pick up this value in Tx
    blocking_read_write_tx(Node, {KeyA, <<"Ac">>}),

    %% <4, 2, 1> || <4, 2, 1> || <2, 1, 1>
    blocking_read_write_tx(Node, [{KeyA, <<"Ad">>}, {KeyB, <<"Bb">>}]),

    %% Two only C
    %% <4, 2, 1> || <4, 2, 1> || <2, 1, 2>
    blocking_read_write_tx(Node, {KeyC, <<"Cb">>}),
    %% <4, 2, 1> || <4, 2, 1> || <2, 1, 3>
    blocking_read_write_tx(Node, {KeyC, <<"Cc">>}),

    %% Four only B
    %% <4, 2, 1> || <4, 3, 1> || <2, 1, 3>
    blocking_read_write_tx(Node, {KeyB, <<"Bc">>}),
    %% <4, 2, 1> || <4, 4, 1> || <2, 1, 3>
    blocking_read_write_tx(Node, {KeyB, <<"Bd">>}),
    %% <4, 2, 1> || <4, 5, 1> || <2, 1, 3>
    blocking_read_write_tx(Node, {KeyB, <<"Be">>}),
    %% <4, 2, 1> || <4, 6, 1> || <2, 1, 3>
    blocking_read_write_tx(Node, {KeyB, <<"Bf">>}),

    %% Now another with the three
    %% <5, 7, 4> || <5, 7, 4> || <5, 7, 4>
    blocking_read_write_tx(Node, [{KeyA, <<"Ae">>}, {KeyB, <<"Bg">>}, {KeyC, <<"Cd">>}])
  ]),

  %% Using <_, 1, 1> at A, we should pick up clock <3,1,1>
  {ok, ReadA} = read_objects(Node, Tx, KeyA),
  ?assertEqual([<<"Ac">>], ReadA),

  %% Repeatable read check
  {ok, ReadBC_2} = read_objects(Node, Tx, [KeyB, KeyC]),
  ?assertEqual([<<"Ba">>, <<"Ca">>], ReadBC_2),

  {ok, []} = commit_transaction(Node, Tx),

  ok.

%% Util functions

execute_parallel(Funs) when is_list(Funs) ->
  S = self(),
  lists:foreach(fun(F) ->
    _ = spawn(fun() -> F(S) end)
  end, Funs),
  collect(length(Funs), []);

execute_parallel(Fun) ->
  [Res] = execute_parallel([Fun]),
  Res.

execute_sequential(Funs) when is_list(Funs) ->
  S = self(),
  lists:foldl(fun(F, Acc) ->
    _ = spawn(fun() -> F(S) end),
    Acc ++ collect(1, [])
  end, [], Funs);

execute_sequential(Fun) ->
  [Res] = execute_sequential([Fun]),
  Res.

execute_blocking_sequential(Funs) when is_list(Funs) ->
  lists:reverse(lists:foldl(fun(F, Acc) ->
    Res = F(),
    timer:sleep(200),
    [Res | Acc]
  end, [], Funs));

execute_blocking_sequential(Fun) ->
  [Res] = execute_blocking_sequential([Fun]),
  Res.

read_write_tx(Node, Assignments) when is_list(Assignments) ->
  Keys = lists:map(fun({Key, _}) -> Key end, Assignments),
  fun(From) ->
    transaction(Node, From, [
      fun(T, _) -> read_objects(Node, T, Keys) end,
      fun(T, _) -> update_objects(Node, T, Assignments) end
    ])
  end;

read_write_tx(Node, Assignment) ->
  read_write_tx(Node, [Assignment]).

blocking_read_write_tx(Node, Assignments) when is_list(Assignments) ->
  Keys = lists:map(fun({Key, _}) -> Key end, Assignments),
  fun() ->
    blocking_transaction(Node, [
      fun(T, _) -> read_objects(Node, T, Keys) end,
      fun(T, _) -> update_objects(Node, T, Assignments) end
    ])
  end;

blocking_read_write_tx(Node, Assignment) ->
  blocking_read_write_tx(Node, [Assignment]).

read_only_tx(Node, Key) ->
  fun(From) ->
    transaction(Node, From, [
      fun(T, _) -> read_objects(Node, T, Key) end
    ])
  end.

blocking_read_only_tx(Node, Key) ->
  fun() ->
    blocking_transaction(Node, [
      fun(T, _) -> read_objects(Node, T, Key) end
    ])
  end.

collect(Processes, Results) ->
  case Processes of
    0 ->
      Results;
    N ->
      receive
        Message -> collect(N - 1, [Message | Results])
      after 1 ->
        collect(N, Results)
      end
  end.

write(Assignments) when is_list(Assignments) ->
  lists:map(fun({K, V}) -> {K, assign, V} end, Assignments);

write({Key, Value}) ->
  [{Key, assign, Value}].

transaction(Node, Dst, Statements) ->
  {ok, T} = start_transaction(Node),
  Last = lists:foldl(fun(F, Acc) -> F(T, Acc) end, [], Statements),
  CT = commit_transaction(Node, T),
  Dst ! {CT, Last}.

blocking_transaction(Node, Statements) ->
  {ok, T} = start_transaction(Node),
  Last = lists:foldl(fun(F, Acc) -> F(T, Acc) end, [], Statements),
  CT = commit_transaction(Node, T),
  {CT, Last}.

start_transaction(Node) ->
  rpc:call(Node, antidote, start_transaction, [ignore, []]).

read_objects(Node, Tx, Keys) when is_list(Keys) ->
  rpc:call(Node, antidote, read_objects, [Keys, Tx]);

read_objects(Node, Tx, Key) ->
  read_objects(Node, Tx, [Key]).

update_objects(Node, Tx, Assignments) ->
  rpc:call(Node, antidote, update_objects, [write(Assignments), Tx]).

commit_transaction(Node, Tx) ->
  rpc:call(Node, antidote, commit_transaction, [Tx]).
