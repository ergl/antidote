%% ETS version-miss tracking
-define(LOG_MISS_TABLE, pvc_log_miss_table).

%% Update the counter. If missing, perform over an zero counter
-define(LOG_VERSION_MISS(Partition),
    _ = ets:update_counter(?LOG_MISS_TABLE, Partition, {2, 1}, {Partition, 0})).
