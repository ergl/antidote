%% Rubis
-define(GROUPING_SEP, <<"+">>).
-define(ID_SEP, <<"@">>).
-define(FIELD_SEP, <<"/">>).

%% General indices
-define(INDEX_SEP, <<"$">>).
-define(UINDEX_SEP, <<"%">>).
-define(INDEX_PAGE_LIMIT, 25).

%% Size of active listener pool for the protocol buffer server
-define(COORD_PB_POOL, (1 * erlang:system_info(schedulers_online))).
%% Port number for protocol buffer sever
-define(COORD_PB_PORT, 7878).

%% Defines how often partition vnodes try to dequeue ready transactions
-define(DEQUEUE_INTERVAL, 5).

%% Storage ETS tables inside antidote_pvc_vnode
-define(DECIDE_TABLE, pvc_decide_table).
-define(MRVC_TABLE, most_recent_vc).
-define(LAST_VSN_TABLE, last_vsn).
-define(PENDING_DATA_TABLE, pending_tx_data).
-define(PENDING_READS_TABLE, pending_reads).
-define(PENDING_WRITES_TABLE, pending_writes).
-define(RC_STORAGE, rc_storage).
