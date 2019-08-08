%% Rubis
-define(GROUPING_SEP, <<"+">>).
-define(ID_SEP, <<"@">>).
-define(FIELD_SEP, <<"/">>).

%% General indices
-define(INDEX_SEP, <<"$">>).
-define(UINDEX_SEP, <<"%">>).
-define(INDEX_PAGE_LIMIT, 25).

%% Amount of Transaction metadata kept for the Version and Commit log
%%
%% VERSION_THRESHOLD specifies the point where the logs will perform a GC
%% The lower this number is set, the more common GC will become.
%%
%% MAX_VERSIONS specifies the number of versions kept in the logs after a GC
%%
%% So, every time VERSION_THRESHOLD versions are reached, we will remove all
%% versions, except for last MAX_VERSIONS.
-define(VERSION_THRESHOLD, 100).
-define(MAX_VERSIONS, 50).

%% Size of active listener pool for the protocol buffer server
-define(COORD_PB_POOL, (1 * erlang:system_info(schedulers_online))).
%% Port number for protocol buffer sever
-define(COORD_PB_PORT, 7878).

%% Defines how often partition vnodes try to dequeue ready transactions
-define(DEQUEUE_INTERVAL, 1000).
