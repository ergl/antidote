#!/usr/bin/env escript
%%! -smp enable -name wwtest@127.0.0.1 -setcookie antidote

-define (KEY(Key), {Key, antidote_crdt_lwwreg, default_bucket}).

main(_) ->
    ww1('antidote@127.0.0.1').

ww1(Node) ->
  ConflictKey = ?KEY(<<"conflicting">>),
  T1 = concurrent(Node, ConflictKey, "foo"),
  T2 = concurrent(Node, ConflictKey, "bar"),
  T3 = fun(From) ->
    transaction(Node, From, [
      fun(T, _) -> rpc:call(Node, antidote, read_objects, [[ConflictKey], T]) end,
      fun(T, {ok, [Results]}) ->
        io:format("Conflicting key contains: ~p~n", [Results]),
        rpc:call(Node, antidote, read_objects, [Results, T])
      end
    ])
  end,
  Res = execute([T1, T2]),
  io:format("Collected ~p from concurrent execution~n", [Res]),
  case Res of
    [{ok, _}, {ok, _}] ->
      execute(T3),
      halt(1);

    [{error, {aborted, _}}, {ok, _}] ->
      halt(0);

    [{ok, _}, {error, {aborted, _}}] ->
      halt(0)
  end.

execute(Funs) when is_list(Funs) ->
  S = self(),
  lists:foreach(fun(F) ->
    _ = spawn(fun() -> F(S) end)
  end, Funs),
  collect(length(Funs), []);

execute(Fun) ->
  [Res] = execute([Fun]),
  Res.

ww(Node) ->
    SummaryKey = ?KEY(<<"$$__SUMMARY__$$">>),

    SetAKey = ?KEY(<<"$$__tableA__ML_KSET__$$">>),
    SetBKey = ?KEY(<<"$$__tableB__ML_KSET__$$">>),

    TableAKey = ?KEY(<<"tableA">>),
    TableBKey = ?KEY(<<"tableB">>),

    T1 = concurrent(Node, SummaryKey, SetAKey, TableAKey),
    T2 = concurrent(Node, SummaryKey, SetBKey, TableBKey),

    T3 = fun(From) ->
        transaction(Node, From, [
            fun(T, _) -> rpc:call(Node, antidote, read_objects, [[SummaryKey], T]) end,
            fun(T, {ok, [Results]}) ->
                io:format("SummaryKey contains: ~p~n", [Results]),
                rpc:call(Node, antidote, read_objects, [Results, T])
            end
        ])
    end,

    S = self(),
    _ = [spawn(fun() -> T1(S) end), spawn(fun() -> T2(S) end)],
    Res = collect(2, []),
    io:format("Collected ~p from concurrent execution~n", [Res]),
    case Res of
        [{ok, _}, {ok, _}] ->
            _ = spawn(fun() -> T3(S) end),
            _ = collect(1, []),
            halt(1);

        [{aborted, _}, {ok, _}] ->
            halt(0);

        [{ok, _}, {aborted, _}] ->
            halt(0)
    end.

concurrent(Node, ConflictingKey, Value) ->
  fun(From) ->
    transaction(Node, From, [
      fun(T, _) -> rpc:call(Node, antidote, read_objects, [[ConflictingKey], T]) end,
      fun(T, _) -> rpc:call(Node, antidote, update_objects, [write({ConflictingKey, Value}), T]) end,
      fun(T, _) -> rpc:call(Node, antidote, read_objects, [[ConflictingKey], T]) end
    ])
  end.

concurrent(Node, SummaryKey, Set, Table) ->
    fun(From) ->
        transaction(Node, From, [
            fun(T, _) -> rpc:call(Node, antidote, read_objects, [[SummaryKey], T]) end,
            fun(T, _) -> rpc:call(Node, antidote, update_objects, [write({SummaryKey, [Set]}), T]) end,
            fun(T, ok) -> rpc:call(Node, antidote, read_objects, [[SummaryKey], T]) end,
            fun(T, {ok, [SetKeys]}) ->
                case SetKeys of
                    [Set] ->
                        rpc:call(Node, antidote, read_objects, [[Set], T])
                end
            end,
            fun(T, _) -> rpc:call(Node, antidote, update_objects, [write({Set, [Table]}), T]) end,
            fun(T, ok) ->
                Value = unicode:characters_to_list(io_lib:format("table ~p schema", [Table])),
                rpc:call(Node, antidote, update_objects, [write({Table, Value}), T])
            end
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
    {ok, T} = rpc:call(Node, antidote, start_transaction, [ignore, []]),
    lists:foldl(fun(F, Acc) -> F(T, Acc) end, [], Statements),
    Dst ! rpc:call(Node, antidote, commit_transaction, [T]).
