%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(antidote_sup).

-behaviour(supervisor).

-include("antidote.hrl").

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type, Args), {I, {I, start_link, Args}, permanent, 5000, Type, [I]}).
-define(VNODE(I, M), {I, {riak_core_vnode_master, start_link, [M]}, permanent, 5000, worker, [riak_core_vnode_master]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    LoggingMaster = {logging_vnode_master,
                     {riak_core_vnode_master, start_link, [logging_vnode]},
                     permanent, 5000, worker, [riak_core_vnode_master]},

    %% RUBIS Key Generator Oracle, one per partition
    RubisKeyGen = ?VNODE(rubis_keygen_vnode_master, rubis_keygen_vnode),

    %% Vnode holding the protocol partition state
    PVCProtocolMaster = ?VNODE(antidote_pvc_vnode_master, antidote_pvc_vnode),

    %% PVC read replicas
    PVCReplicaSup = ?CHILD(pvc_read_replica_sup, supervisor, []),

    %% Exometer and other metrics collector
    StatsCollector = ?CHILD(antidote_stats_collector, worker, []),

    ClockSIMaster = ?VNODE(clocksi_vnode_master, clocksi_vnode),
    ClockSIiTxCoordSup = {clocksi_interactive_tx_coord_sup,
                          {clocksi_interactive_tx_coord_sup, start_link, []},
                          permanent, 5000, supervisor,  [clockSI_interactive_tx_coord_sup]},

    ClockSIReadSup = {clocksi_readitem_sup,
                      {clocksi_readitem_sup, start_link, []},
                      permanent, 5000, supervisor, [clocksi_readitem_sup]},

    MaterializerMaster = ?VNODE(materializer_vnode_master, materializer_vnode),

    BCounterManager = ?CHILD(bcounter_mgr, worker, []),

    ZMQContextManager = ?CHILD(zmq_context, worker, []),
    InterDcPub = ?CHILD(inter_dc_pub, worker, []),
    InterDcSub = ?CHILD(inter_dc_sub, worker, []),
    StableMetaData = ?CHILD(stable_meta_data_server, worker, []),
    InterDcSubVnode = ?VNODE(inter_dc_sub_vnode_master, inter_dc_sub_vnode),
    InterDcDepVnode = ?VNODE(inter_dc_dep_vnode_master, inter_dc_dep_vnode),
    InterDcLogReaderQMaster = ?CHILD(inter_dc_query, worker, []),
    InterDcLogReaderRMaster = ?CHILD(inter_dc_query_receive_socket, worker, []),
    InterDcLogSenderMaster = ?VNODE(inter_dc_log_sender_vnode_master, inter_dc_log_sender_vnode),


    MetaDataManagerSup = {meta_data_manager_sup,
                          {meta_data_manager_sup, start_link, [stable]},
                          permanent, 5000, supervisor,
                          [meta_data_manager_sup]},

    MetaDataSenderSup = {meta_data_sender_sup,
                         {meta_data_sender_sup, start_link, [stable_time_functions:export_funcs_and_vals()]},
                         permanent, 5000, supervisor, [meta_data_sender_sup]},

    LogResponseReaderSup = {inter_dc_query_response_sup,
                            {inter_dc_query_response_sup, start_link, [?INTER_DC_QUERY_CONCURRENCY]},
                            permanent, 5000, supervisor, [inter_dc_query_response_sup]},


    {ok,
     {{one_for_one, 5, 10},
      [LoggingMaster,
       RubisKeyGen,
       PVCProtocolMaster,
       PVCReplicaSup,
       StatsCollector,
       ClockSIMaster,
       ClockSIiTxCoordSup,
       ClockSIReadSup,
       MaterializerMaster,
       ZMQContextManager,
       InterDcPub,
       InterDcSub,
       InterDcSubVnode,
       InterDcDepVnode,
       InterDcLogReaderQMaster,
       InterDcLogReaderRMaster,
       InterDcLogSenderMaster,
       StableMetaData,
       MetaDataManagerSup,
       MetaDataSenderSup,
       BCounterManager,
       LogResponseReaderSup]}}.
