#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name cluster_tool@127.0.0.1 -setcookie antidote

%%-mode(compile).

%%main(Command) ->
%%  ok.

-define(COMMANDS, [{partition}, {heal}]).

main(ArgList) ->
  case parse_arg_list(ArgList) of
    error ->
      usage();

      {ok, Command, Groups} ->
        do_command(Command, Groups)
    end.

parse_arg_list([StrCommand | StrNodes]) ->
  AtomCommand = list_to_atom(StrCommand),
  case lists:keyfind(AtomCommand, 1, ?COMMANDS) of
    false ->
      error;
    {Command} ->
      case parse_node_list(StrNodes) of
        error ->
          error;
        {ok, Nodes} ->
          parse_command(Command, Nodes)
      end
  end;

parse_arg_list(_) ->
  error.

-spec parse_node_list(list(string())) -> {ok, [node()]} | error.
parse_node_list([]) ->
  error;

parse_node_list([_|_]=NodeListString) ->
  try
    Nodes = lists:foldl(fun(NodeString, Acc) ->
      Node = list_to_atom(NodeString),
      [Node | Acc]
    end, [], NodeListString),
    {ok, lists:reverse(Nodes)}
  catch
    _:_ -> error
  end.

parse_command(Command, Nodes) ->
  N = length(Nodes),
  {GroupA, GroupB} = lists:split(N div 2, Nodes),
  {ok, Command, {GroupA, GroupB}}.

usage() ->
  Name = filename:basename(escript:script_name()),
  io:fwrite(standard_error, "~s <command> 'node_1@host_1' ... 'node_n@host_n'~n", [Name]),
  halt(1).

do_command(partition, {GroupA, GroupB}) ->
  partition_cluster(GroupA, GroupB);

do_command(heal, {GroupA, GroupB}) ->
  heal_cluster(GroupA, GroupB).

partition_cluster(GroupA, GroupB) ->
  %% For all combinations in GroupA with GroupB
  Res = pmap(fun({NodeA, NodeB}) ->
    %% Separate A from B with cookies
    true = rpc:call(NodeA, erlang, set_cookie, [NodeB, canttouchthis]),
    %% Make B believe that A crashed
    true = rpc:call(NodeA, erlang, disconnect_node, [NodeB]),
    ok = wait_until_disconnected(NodeA, NodeB)
  end, combine(GroupA, GroupB)),
  lists:all(fun(ok) -> true; (_) -> false end, Res).

heal_cluster(GroupA, GroupB) ->
  GoodCookie = erlang:get_cookie(),
  %% For all combinations in GroupA with GroupB
  Res = pmap(fun({NodeA, NodeB}) ->
    %% Authenticate again
    true = rpc:call(NodeA, erlang, set_cookie, [NodeB, GoodCookie]),
    ok = wait_until_connected(NodeA, NodeB)
  end, combine(GroupA, GroupB)),
  lists:all(fun(ok) -> true; (_) -> false end, Res).

%% Util functions

wait_until_disconnected(NodeA, NodeB) ->
  wait_until(fun() ->
    pang =:= rpc:call(NodeA, net_adm, ping, [NodeB])
  end).

wait_until_connected(NodeA, NodeB) ->
  wait_until(fun() ->
    pong =:= rpc:call(NodeA, net_adm, ping, [NodeB])
  end).

%% @doc Utility function used to construct test predicates. Retries the
%%      function `Fun' until it returns `true', or until the maximum
%%      number of retries is reached.
%%
%% @TODO Use config for this
-spec wait_until(fun(() -> boolean())) -> ok | {fail, boolean()}.
wait_until(Fun) when is_function(Fun) ->
  MaxTime = 600000,
  Delay = 1000,
  Retry = MaxTime div Delay,
  wait_until(Fun, Retry, Delay).

-spec wait_until(
    fun(() -> boolean()),
    non_neg_integer(),
    non_neg_integer()
) -> ok | {fail, boolean()}.

wait_until(Fun, Retry, Delay) when Retry > 0 ->
  wait_until_result(Fun, true, Retry, Delay).

-spec wait_until_result(
    fun(() -> any()),
    any(),
    non_neg_integer(),
    non_neg_integer()
) -> ok | {fail, any()}.

wait_until_result(Fun, Result, Retry, Delay) when Retry > 0 ->
  Res = Fun(),
  case Res of
    Result ->
      ok;

    _ when Retry == 1 ->
      {fail, Res};

    _ ->
      timer:sleep(Delay),
      wait_until_result(Fun, Result, Retry-1, Delay)
  end.

pmap(Fun, List) ->
  Parent = self(),
  Total = lists:foldl(fun(X, N) ->
    spawn_link(fun() ->
      Parent ! {pmap, N, Fun(X)}
    end),
    N + 1
  end, 0, List),
  collect(Total, []).

collect(0, Acc) ->
  element(2, lists:unzip(lists:keysort(1, Acc)));

collect(N, Acc) ->
  receive
    {pmap, Seq, R} ->
      collect(N - 1, [{Seq, R} | Acc])
  end.

combine(A, B) ->
  [{L, R} || L <- A, R <- B].
