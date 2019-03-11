%% Ignore log calls unless in debug artifact
-ifdef(debug_log).
-define(LAGER_LOG(String, Args), lager:info(String, Args)).
-else.
-define(LAGER_LOG(_String, Args), _ = Args).
-endif.
