%% ETS version-miss tracking
-define(LOG_MISS_TABLE, pvc_log_miss_table).
-define(LOG_VERSION_MISS(Partition),
    _ = ets:update_counter(pvc_log_miss_table, Partition, {2, 1}, {Partition, 0})).
