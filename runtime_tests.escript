#!/usr/bin/env escript
%%! -smp enable -name runtimetests@127.0.0.1 -setcookie antidote

-include_lib("eunit/include/eunit.hrl").

-define (KEY(Key, Bucket), {Key, antidote_crdt_lwwreg, Bucket}).
-define (KEY(Key), ?KEY(Key, default_bucket)).

-define (TESTS, [
  fun(N) -> write_write_check(N) end
]).

main(_) ->
  lists:foreach(fun(El) ->
    ok = El('antidote@127.0.0.1')
  end, ?TESTS).

write_write_check(Node) ->
  ConflictKey = ?KEY(conflict),
  Res = execute([
    read_write_tx(Node, ConflictKey, 1),
    read_write_tx(Node, ConflictKey, 2)
  ]),
  ?assertNotMatch([{ok, _}, {ok, _}], Res),
  ?assert(lists:any(fun({error, {aborted, _}}) -> true; (_) -> false end, Res)).

execute(Funs) when is_list(Funs) ->
  S = self(),
  lists:foreach(fun(F) ->
    _ = spawn(fun() -> F(S) end)
  end, Funs),
  collect(length(Funs), []);

execute(Fun) ->
  [Res] = execute([Fun]),
  Res.

read_write_tx(Node, Key, Value) ->
  fun(From) ->
    transaction(Node, From, [
      fun(T, _) -> read_objects(Node, T, Key) end,
      fun(T, _) -> update_objects(Node, T, {Key, Value}) end
    ])
  end.

read_objects(Node, Tx, Keys) when is_list(Keys) ->
  rpc:call(Node, antidote, read_objects, [Keys, Tx]);

read_objects(Node, Tx, Key) ->
  read_objects(Node, Tx, [Key]).

update_objects(Node, Tx, Assingments) ->
  rpc:call(Node, antidote, update_objects, [write(Assingments), Tx]).

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
  {ok, T} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
  lists:foldl(fun(F, Acc) -> F(T, Acc) end, [], Statements),
  Dst ! rpc:call(Node, antidote, commit_transaction, [T]).
