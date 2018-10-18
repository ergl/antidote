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

%% Protocol-specific

%% Time (in ms) a partition should wait between retries at checking
%% a partition's most recent vc during reads.
-define(PVC_WAIT_MS, 1000).

%% Vector-clock specific type
-type pvc_vc() :: pvc_vclock:vc().

-record(txn, {
    %% VC representing the minimum snapshot version that must be read
    vc_aggr :: pvc_vc(),
    %% VC representing the causal dependencies picked up during execution
    vc_dep :: pvc_vc(),
    %% Represents the partitions where the transaction has fixed a snapshot
    has_read :: sets:set(chash:index_as_int()),
    id :: txn_id()
}).

-record(txn_id, {
    %% Pid of the coordinator fsm
    server_pid :: pid()
}).

-type txn() :: #txn{}.
-type txn_id() :: #txn_id{}.

%% Type of the writset
-type ws() :: orddict:orddict(term(), term()).

%% Set of partitions being updated in a transaction
-type partitions() :: orddict:orddict(chash:index_as_int(), ws()).

%% Buffer with the keys that are going to be indexed
-type index_buffer() :: orddict:orddict({chash:index_as_int(), node()}, ordsets:ordset(term())).
