#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name flush_queues@127.0.0.1 -setcookie antidote

-mode(compile).

-export([main/1]).

main([NodeNameListConfig]) ->
    validate(parse_node_config(NodeNameListConfig)).

%% @doc Parse node names from config file
%%
%% The config file is the same as the cluster definition.
-spec parse_node_config(ConfigFilePath :: string()) -> {ok, [atom()]} | error.
parse_node_config(ConfigFilePath) ->
    case file:consult(ConfigFilePath) of
        {ok, Terms} ->
            {clusters, ClusterMap} = lists:keyfind(clusters, 1, Terms),
            NodeNames = lists:usort(lists:flatten(maps:values(ClusterMap))),
            {ok, build_erlang_node_names(NodeNames)};
        _ ->
            error
    end.

-spec build_erlang_node_names([atom()]) -> [atom()].
build_erlang_node_names(NodeNames) ->
    [begin
        {ok, Addr} = inet:getaddr(Node, inet),
        IPString = inet:ntoa(Addr),
        list_to_atom("antidote@" ++ IPString)
    end || Node <- NodeNames].

%% @doc Validate parsing, then proceed
validate(error) ->
    usage();
validate({ok, Nodes}) when is_list(Nodes) ->
    io:format("Flushing commit queues of nodes ~p~n", [Nodes]),
    lists:foreach(fun(N) -> erlang:set_cookie(N, antidote) end, Nodes),
    {_, BadNodes} = rpc:multicall(Nodes, inter_dc_manager, flush_pvc_commit_queues, [], infinity),
    case BadNodes of
        [] ->
            halt(0);
        _ ->
            io:fwrite(standard_error, "flush_pvc_commit_queues failed on ~p, aborting", [BadNodes]),
            halt(1)
    end.

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(standard_error, "~s <config_file>~n", [Name]),
    halt(1).
