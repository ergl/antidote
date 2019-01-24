%% Rubis
-define(GROUPING_SEP, <<"+">>).
-define(ID_SEP, <<"@">>).
-define(FIELD_SEP, <<"/">>).

%% General indices
-define(INDEX_SEP, <<"$">>).
-define(UINDEX_SEP, <<"%">>).
-define(INDEX_PAGE_LIMIT, 25).

%% Amount of Transaction metadata kept
-define(VERSION_THRESHOLD, 500).
-define(MAX_VERSIONS, 100).

%% Amount of remote reader server processes
-define(NUM_READERS, 750).

%% Size of active listener pool for the protocol buffer server
-define(RUBIS_PB_POOL, 100).
%% Port number for protocol buffer sever
-define(RUBIS_PB_PORT, 7878).
