#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name report_version_misses@127.0.0.1 -setcookie antidote

-mode(compile).

-export([main/1]).

main([NodeNameListConfig]) ->
    validate(parse_node_config(NodeNameListConfig));
main(_) ->
    usage(),
    halt(1).

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
    io:format("Getting report on vlog misses for nodes ~p~n", [Nodes]),
    lists:foreach(fun(N) -> erlang:set_cookie(N, antidote) end, Nodes),
    {Results, BadNodes} = rpc:multicall(Nodes, antidote_stats_collector, report_vlog_misses, [], infinity),

    BadNodes =:= [] orelse begin
        io:fwrite(standard_error, "report_version_misses failed on ~p~n", [BadNodes]),
        true
    end,

    io:fwrite(standard_io, "Misses~n~p~n", [Results]),
    halt(0).

-spec usage() -> no_return().
usage() ->
    Name = filename:basename(escript:script_name()),
    io:fwrite(standard_error, "~s <config_file>~n", [Name]),
    halt(1).
