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
-module(materializer_vnode).

-behaviour(riak_core_vnode).

-include("antidote.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%% Number of snapshots to trigger GC
-define(SNAPSHOT_THRESHOLD, 10).
%% Number of snapshots to keep after GC
-define(SNAPSHOT_MIN, 5).
%% Number of ops to keep before GC
-define(OPS_THRESHOLD, 50).
%% The first 3 elements in operations list are meta-data
%% First is the key
%% Second is a tuple {current op list size, max op list size}
%% Thrid is a counter that assigns each op 1 larger than the previous
%% Fourth is where the list of ops start
-define(FIRST_OP, 4).
%% If after the op GC there are only this many or less spaces
%% free in the op list then increase the list size
-define(RESIZE_THRESHOLD, 5).
%% Expected time to wait until the logging vnode is up
-define(LOG_STARTUP_WAIT, 1000).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([start_vnode/1,
    check_tables_ready/0,
    read/6,
    get_cache_name/2,
    store_ss/3,
    update/3,
    op_not_already_in_snapshot/2]).

%% Callbacks
-export([init/1,
    terminate/2,
    handle_command/3,
    is_empty/1,
    delete/1,
    handle_handoff_command/3,
    handoff_starting/2,
    handoff_cancelled/1,
    handoff_finished/2,
    handle_handoff_data/2,
    encode_handoff_item/2,
    handle_coverage/4,
    handle_exit/3]).

-record(state, {
    partition :: partition_id(),
    ops_cache :: cache_id(),
    snapshot_cache :: cache_id(),
    is_ready :: boolean()}).

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

%% @doc Read state of key at given snapshot time, this does not touch the vnode process
%%      directly, instead it just reads from the operations and snapshot tables that
%%      are in shared memory, allowing concurrent reads.
-spec read(key(), type(), transaction(),cache_id(), cache_id(), partition_id()) -> {ok, snapshot()} | {error, reason()}.
read(Key, Type, Transaction,OpsCache,SnapshotCache,Partition) ->
    case ets:info(OpsCache) of
        undefined ->
            riak_core_vnode_master:sync_command({Partition,node()},
                {read,Key,Type,Transaction},
                materializer_vnode_master,
                infinity);
        _ ->
            internal_read(Key, Type, Transaction, OpsCache, SnapshotCache)
    end.

-spec get_cache_name(non_neg_integer(),atom()) -> atom().
get_cache_name(Partition,Base) ->
    list_to_atom(atom_to_list(Base) ++ "-" ++ integer_to_list(Partition)).

%%@doc write operation to cache for future read, updates are stored
%%     one at a time into the ets tables
-spec update(key(), operation_payload(), transaction()) -> ok | {error, reason()}.
update(Key, DownstreamOp, Transaction) ->
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:sync_command(IndexNode, {update, Key, DownstreamOp, Transaction},
        materializer_vnode_master).

%%@doc write snapshot to cache for future read, snapshots are stored
%%     one at a time into the ets table
-spec store_ss(key(), snapshot(), snapshot_time() | {snapshot_time(), snapshot_time()}) -> ok.
store_ss(Key, Snapshot, Params) ->
%%    lager:info("key = ~p",[Key]),
    Preflist = log_utilities:get_preflist_from_key(Key),
    IndexNode = hd(Preflist),
    riak_core_vnode_master:command(IndexNode, {store_ss,Key, Snapshot, Params},
        materializer_vnode_master).

init([Partition]) ->
    OpsCache = open_table(Partition, ops_cache),
    SnapshotCache = open_table(Partition, snapshot_cache),
    IsReady = case application:get_env(antidote,recover_from_log) of
                  {ok, true} ->
                      lager:info("Checking for logs to init materializer ~p", [Partition]),
                      riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
                      false;
                  _ ->
                      true
              end,
    {ok, #state{is_ready = IsReady, partition=Partition, ops_cache=OpsCache, snapshot_cache=SnapshotCache}}.

-spec load_from_log_to_tables(partition_id(), ets:tid(), ets:tid()) -> ok | {error, reason()}.
load_from_log_to_tables(Partition, OpsCache, SnapshotCache) ->
    LogId = [Partition],
    Node = {Partition, log_utilities:get_my_node(Partition)},
    loop_until_loaded(Node, LogId, start, dict:new(), OpsCache, SnapshotCache).

-spec loop_until_loaded({partition_id(), node()}, log_id(), start | disk_log:continuation(), dict(), ets:tid(), ets:tid()) -> ok | {error, reason()}.
loop_until_loaded(Node, LogId, Continuation, Ops, OpsCache, SnapshotCache) ->
    case logging_vnode:get_all(Node, LogId, Continuation, Ops) of
        {error, Reason} ->
            {error, Reason};
        {NewContinuation, NewOps, OpsDict} ->
            load_ops(OpsDict, OpsCache, SnapshotCache),
            loop_until_loaded(Node, LogId, NewContinuation, NewOps, OpsCache, SnapshotCache);
        {eof, OpsDict} ->
            load_ops(OpsDict, OpsCache, SnapshotCache),
            ok
    end.

-spec load_ops(dict(), ets:tid(), ets:tid()) -> true.
load_ops(OpsDict, OpsCache, SnapshotCache) ->
    dict:fold(fun(Key, CommittedOps, _Acc) ->
        lists:foreach(fun({_OpId,Op}) ->
            #operation_payload{key = Key} = Op,
            case op_insert_gc(Key, Op, OpsCache, SnapshotCache, no_txn_inserting_from_log) of
                ok ->
                    true;
                {error, Reason} ->
                    {error, Reason}
            end
                      end, CommittedOps)
              end, true, OpsDict).

-spec open_table(partition_id(), 'ops_cache' | 'snapshot_cache') -> atom() | ets:tid().
open_table(Partition, Name) ->
    case ets:info(get_cache_name(Partition, Name)) of
        undefined ->
            ets:new(get_cache_name(Partition, Name),
                [set, protected, named_table, ?TABLE_CONCURRENCY]);
        _ ->
            %% Other vnode hasn't finished closing tables
            lager:info("Unable to open ets table in materializer vnode, retrying"),
            timer:sleep(100),
            try
                ets:delete(get_cache_name(Partition, Name))
            catch
                _:_Reason->
                    ok
            end,
            open_table(Partition, Name)
    end.

%% @doc The tables holding the updates and snapshots are shared with concurrent
%%      readers, allowing them to be non-blocking and concurrent.
%%      This function checks whether or not all tables have been intialized or not yet.
%%      Returns true if the have, false otherwise.
-spec check_tables_ready() -> boolean().
check_tables_ready() ->
    {ok, CHBin} = riak_core_ring_manager:get_chash_bin(),
    PartitionList = chashbin:to_list(CHBin),
    check_table_ready(PartitionList).

-spec check_table_ready([{partition_id(),node()}]) -> boolean().
check_table_ready([]) ->
    true;
check_table_ready([{Partition,Node}|Rest]) ->
    Result =
        try
            riak_core_vnode_master:sync_command({Partition,Node},
                {check_ready},
                materializer_vnode_master,
                infinity)
        catch
            _:_Reason ->
                false
        end,
    case Result of
        true ->
            check_table_ready(Rest);
        false ->
            false
    end.

handle_command({hello}, _Sender, State) ->
    {reply, ok, State};

handle_command({check_ready},_Sender,State = #state{partition=Partition, is_ready=IsReady}) ->
    Result = case ets:info(get_cache_name(Partition,ops_cache)) of
                 undefined ->
                     false;
                 _ ->
                     case ets:info(get_cache_name(Partition,snapshot_cache)) of
                         undefined ->
                             false;
                         _ ->
                             true
                     end
             end,
    Result2 = Result and IsReady,
    {reply, Result2, State};

handle_command({read, Key, Type, Transaction}, _Sender,
  State = #state{ops_cache = OpsCache, snapshot_cache=SnapshotCache,partition=Partition})->
    {reply, read(Key, Type, Transaction,OpsCache,SnapshotCache,Partition), State};

handle_command({update, Key, DownstreamOp, Transaction}, _Sender,
  State = #state{ops_cache = OpsCache, snapshot_cache=SnapshotCache})->
    case op_insert_gc(Key,DownstreamOp, OpsCache, SnapshotCache, Transaction) of
        ok ->
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;



handle_command({store_ss, Key, Snapshot, Params}, _Sender,
  State = #state{ops_cache = OpsCache, snapshot_cache=SnapshotCache})->
    internal_store_ss(Key,Snapshot,Params,OpsCache,SnapshotCache,false),
    {noreply, State};

handle_command(load_from_log, _Sender, State=#state{partition=Partition,
    ops_cache=OpsCache,
    snapshot_cache=SnapshotCache}) ->
    IsReady = try
                  case load_from_log_to_tables(Partition, OpsCache, SnapshotCache) of
                      ok ->
                          lager:info("Finished loading from log to materializer on partition ~w", [Partition]),
                          true;
                      {error, not_ready} ->
                          false;
                      {error, Reason} ->
                          lager:error("Unable to load logs from disk: ~w, continuing", [Reason]),
                          true
                  end
              catch
                  Error:Reason1 ->
                      lager:info("Error loading from log ~w~n~w, will retry", [Error, Reason1]),
                      false
              end,
    ok = case IsReady of
             false ->
                 riak_core_vnode:send_command_after(?LOG_STARTUP_WAIT, load_from_log),
                 ok;
             true ->
                 ok
         end,
    {noreply, State#state{is_ready=IsReady}};

handle_command(_Message, _Sender, State) ->
    {noreply, State}.

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0},
  _Sender,
  State = #state{ops_cache = OpsCache}) ->
    F = fun(Key, A) ->
        [Key1|_] = tuple_to_list(Key),
        Fun(Key1, Key, A)
        end,
    Acc = ets:foldl(F, Acc0, OpsCache),
    {reply, Acc, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State=#state{ops_cache=OpsCache}) ->
    {_Key, Operation} = binary_to_term(Data),
    true = ets:insert(OpsCache, Operation),
    {reply, ok, State}.

encode_handoff_item(Key, Operation) ->
    term_to_binary({Key, Operation}).

is_empty(State=#state{ops_cache=OpsCache}) ->
    case ets:first(OpsCache) of
        '$end_of_table' ->
            {true, State};
        _ ->
            {false, State}
    end.

delete(State=#state{ops_cache=_OpsCache}) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State=#state{ops_cache=OpsCache,snapshot_cache=SnapshotCache}) ->
    try
        ets:delete(OpsCache),
        ets:delete(SnapshotCache)
    catch
        _:_Reason->
            ok
    end,
    ok.



%%---------------- Internal Functions -------------------%%

%% todo: check the definition
-spec internal_store_ss(key(), snapshot(), snapshot_time() | {snapshot_time(), snapshot_time()}, cache_id(), cache_id(), false) -> true.
internal_store_ss(Key, Snapshot, SnapshotParams, OpsCache, SnapshotCache, ShouldGc) ->
    Protocol = application:get_env(antidote, txn_prot),
    SnapshotDict = case ets:lookup(SnapshotCache, Key) of
                       [] ->
                           vector_orddict:new();
                       [{_, SnapshotDictA}] ->
                           SnapshotDictA
                   end,
    SnapshotDict1 = vector_orddict:insert_bigger(SnapshotParams, Snapshot, SnapshotDict, Protocol),
    snapshot_insert_gc(Key, SnapshotDict1, SnapshotCache, OpsCache, ShouldGc, Protocol).

%% @doc This function takes care of reading. It is implemented here for not blocking the
%% vnode when the write function calls it. That is done for garbage collection.
-spec internal_read(key(), type(), transaction(), cache_id(), cache_id()) -> {ok, {snapshot(), any()}}| {error, no_snapshot}.
internal_read(Key, Type, Transaction, OpsCache, SnapshotCache) ->
    internal_read(Key, Type, Transaction, OpsCache, SnapshotCache,false).
internal_read(Key, Type, Transaction, OpsCache, SnapshotCache, ShouldGc) ->
    TxnId = Transaction#transaction.txn_id,
    Protocol = Transaction#transaction.transactional_protocol,
    case ets:lookup(OpsCache, Key) of
        [] ->
            {LatestCompatSnapshot, SnapshotCommitParams} = create_empty_snapshot(Transaction, Type),
            {ok, {LatestCompatSnapshot, SnapshotCommitParams}};
        [Tuple] ->
            {Key, Len, _OpId, _ListLen, OperationsForKey} = tuple_to_key(Tuple),
            {UpdatedTxnRecord, TempCommitParameters} =
                case Protocol of
                    physics ->
                        case TxnId of no_txn_inserting_from_log -> {Transaction, empty};
                            _ ->
                                case define_snapshot_vc_for_transaction(Transaction, OperationsForKey) of
                                    OpCommitParams = {OperationCommitVC, _OperationDependencyVC, _ReadVC} ->
                                        {Transaction#transaction{snapshot_vc = OperationCommitVC}, OpCommitParams};
                                    no_operation_to_define_snapshot ->
                                        lager:info("there no_operation_to_define_snapshot"),
                                        JokerVC = Transaction#transaction.physics_read_metadata#physics_read_metadata.dep_upbound,
                                        {Transaction#transaction{snapshot_vc = JokerVC}, {JokerVC, JokerVC, JokerVC}}
                                end
                        end;
                    Protocol when ((Protocol == clocksi) or (Protocol == gr)) ->
                        {Transaction, empty}
                end,
            Result = case ets:lookup(SnapshotCache, Key) of
                         [] ->
                             %% First time reading this key, store an empty snapshot in the cache
                             BlankSS = {0, clocksi_materializer:new(Type)},
                             case TxnId of %%Why do we need this?
                                 Txid1 when ((Txid1 == eunit_test) orelse (Txid1 == no_txn_inserting_from_log)) ->
                                     internal_store_ss(Key, BlankSS, vectorclock:new(), OpsCache, SnapshotCache, false);
                                 _ ->
                                     materializer_vnode:store_ss(Key, BlankSS, vectorclock:new())
                             end,
                             {BlankSS, ignore, true};
                         [{_, SnapshotDict}] ->
                             case vector_orddict:get_smaller(UpdatedTxnRecord#transaction.snapshot_vc, SnapshotDict) of
                                 {undefined, _IsF} ->
                                     {error, no_snapshot};
                                 {{LS, SCP}, IsF} ->
                                     {LS, SCP, IsF}
                             end
                     end,
            {Length, Ops, {LastOp, LatestSnapshot}, SnapshotCommitTime, IsFirst} =
                case Result of
                    {error, no_snapshot} ->
                        lager:info("no snapshot in the cache for key: ~p",[Key]),
                        LogId = log_utilities:get_logid_from_key(Key),
                        [Node] = log_utilities:get_preflist_from_key(Key),
                        Res = logging_vnode:get(Node, LogId, UpdatedTxnRecord, Type, Key),
                        Res;
%%                        {0, {error, no_snapshot}, {foo, foo}, foo, foo};
                    {LatestSnapshot1, SnapshotCommitTime1, IsFirst1} ->
                        {Len, OperationsForKey, LatestSnapshot1, SnapshotCommitTime1, IsFirst1}
                end,
            case Length of
                0 ->
                            {ok, {LatestSnapshot, SnapshotCommitTime}};
%%                    lager:info("materializer_vnode: line 489 IS THIS POSSIBLE?"),
                _ ->
                    case clocksi_materializer:materialize(Type, LatestSnapshot, LastOp, SnapshotCommitTime, UpdatedTxnRecord, Ops) of
                        {ok, Snapshot, NewLastOp, CommitTime, NewSS} ->
                            %% the following checks for the case there were no snapshots and there were operations, but none was applicable
                            %% for the given snapshot_time
                            %% But is the snapshot not safe?
                            case CommitTime of
                                ignore ->
                                    {ok, {Snapshot, CommitTime}};
                                _ ->
                                    case (NewSS and IsFirst) orelse ShouldGc of
                                        %% Only store the snapshot if it would be at the end of the list and has new operations added to the
                                        %% previous snapshot
                                        true ->
                                            case TxnId of
                                                Txid when ((Txid == eunit_test) orelse (Txid == no_txn_inserting_from_log)) ->
                                                    internal_store_ss(Key, {NewLastOp, Snapshot}, CommitTime, OpsCache, SnapshotCache, ShouldGc);
                                                _ ->
                                                    store_ss(Key, {NewLastOp, Snapshot}, CommitTime)
                                            end;
                                        _ ->
                                            ok
                                    end,
                                    FinalCommitParameters = case Protocol of
                                        physics ->
                                            TempCommitParameters;
                                        Protocol when ((Protocol == clocksi) or (Protocol == gr)) ->
                                            CommitTime
                                    end,
                                    {ok, {Snapshot, FinalCommitParameters}}
                            end;
                        {error, Reason} ->
                            {error, Reason}
                    end
            end
    end.

%% @doc This fuction is used by the causally consistent cut for defining
%% which is the latest operation that is compatible with the snapshot
%% the protocol uses the commit time of the operation as the "snapshot time"
%% of this particular read, whithin the transaction.
define_snapshot_vc_for_transaction(_Transaction, []) ->
    no_operation_to_define_snapshot;
define_snapshot_vc_for_transaction(Transaction, OperationList) ->
    LocalDCReadTime = clocksi_vnode:now_microsec(now()),
    define_snapshot_vc_for_transaction(Transaction, OperationList, LocalDCReadTime, ignore).

define_snapshot_vc_for_transaction(_Transaction, [], _LocalDCReadTime, _ReadVC) ->
    no_compatible_operation_found;
define_snapshot_vc_for_transaction(Transaction, [Operation | Rest], LocalDCReadTime, ReadVC) ->
    {_OpId, Op} = Operation,
    TxCTLowBound = Transaction#transaction.physics_read_metadata#physics_read_metadata.commit_time_lowbound,
    TxDepUpBound = Transaction#transaction.physics_read_metadata#physics_read_metadata.dep_upbound,
    OperationDependencyVC = Op#operation_payload.dependency_vc,
    {OperationDC, OperationCommitTime} = Op#operation_payload.dc_and_commit_time,
    OperationCommitVC = vectorclock:create_commit_vector_clock(OperationDC, OperationCommitTime, OperationDependencyVC),
%%    lager:info("~nOperationCommitVC =~p~n TxCTLowBound =~p~n OperationDependencyVC =~p~n TxDepUpBound =~p~n",
%%        [OperationCommitVC, TxCTLowBound, OperationDependencyVC, TxDepUpBound]),
    FinalReadVC = case ReadVC of
                      ignore -> %% newest operation in the list.
                          OPCommitVCLocalDC = vectorclock:get_clock_of_dc(dc_utilities:get_my_dc_id(), OperationCommitVC),
                          vectorclock:set_clock_of_dc(OperationDC, max(LocalDCReadTime, OPCommitVCLocalDC), OperationDependencyVC);
                      _ ->
                          ReadVC
                  end,

    case vector_orddict:is_causally_compatible(FinalReadVC, TxCTLowBound, OperationDependencyVC, TxDepUpBound) of
        true ->
            {OperationCommitVC, OperationDependencyVC, FinalReadVC};
        false ->
            NewOperationCommitVC = vectorclock:set_clock_of_dc(OperationDC, OperationCommitTime - 1, OperationCommitVC),
            define_snapshot_vc_for_transaction(Transaction, Rest, LocalDCReadTime, NewOperationCommitVC)
    end.

%%%% Todo: Future: Implement the following function for a causal snapshot
%%get_all_operations_from_log_for_key(Key, Type, Transaction) ->
%%    case Transaction#transaction.transactional_protocol of
%%        physics->
%%            {{_LastOp, _LatestCompatSnapshot}, _SnapshotCommitParams, _IsFirst} =
%%                {{0, Type:new()}, {vectorclock:new(),vectorclock:new(), clocksi_vnode:now_microsec(now())}, false};
%%        Protocol when ((Protocol == gr) or (Protocol == clocksi))->
%%            LogId = log_utilities:get_logid_from_key(Key),
%%            [Node] = log_utilities:get_preflist_from_key(Key),
%%%%            {{_LastOp, _LatestCompatSnapshot}, _SnapshotCommitParams, _IsFirst} = logging_vnode:get(Node, LogId, Transaction, Type, Key)
%%            {_Lenght, _CommittedOpsForKey} = logging_vnode:get(Node, LogId, Transaction, Type, Key)
%%    end.
create_empty_snapshot(Transaction, Type) ->
    case Transaction#transaction.transactional_protocol of
        physics ->
            ReadTime = clocksi_vnode:now_microsec(now()),
            MyDc = dc_utilities:get_my_dc_id(),
            ReadTimeVC = vectorclock:set_clock_of_dc(MyDc, ReadTime, vectorclock:new()),
            {clocksi_materializer:new(Type), {vectorclock:new(), vectorclock:new(), ReadTimeVC}};
        Protocol when ((Protocol == gr) or (Protocol == clocksi)) ->
            {clocksi_materializer:new(Type), vectorclock:new()}
    end.

%% returns true if op is more recent than SS (i.e. is not in the ss)
%% returns false otw
-spec op_not_already_in_snapshot(snapshot_time() | ignore, vectorclock()) -> boolean().
op_not_already_in_snapshot(ignore, _) ->
    true;
op_not_already_in_snapshot(_, ignore) ->
    true;
op_not_already_in_snapshot(_, empty) ->
    true;
op_not_already_in_snapshot(empty, _) ->
    true;
op_not_already_in_snapshot(SSTime, CommitVC) ->
    not vectorclock:le(CommitVC, SSTime).


%% @doc Operation to insert a Snapshot in the cache and start
%%      Garbage collection triggered by reads.
-spec snapshot_insert_gc(key(), vector_orddict:vector_orddict(),
  cache_id(),cache_id(),boolean(), atom()) -> true.
snapshot_insert_gc(Key, SnapshotDict, SnapshotCache, OpsCache, ShouldGc, Protocol) ->
    %% Should check op size here also, when run from op gc
    case ((vector_orddict:size(SnapshotDict)) >= ?SNAPSHOT_THRESHOLD) orelse ShouldGc of
        true ->
            %% snapshots are no longer totally ordered
            PrunedSnapshots = vector_orddict:sublist(SnapshotDict, 1, ?SNAPSHOT_MIN),
            FirstOp = vector_orddict:last(PrunedSnapshots),
            {CT, _S} = FirstOp,
                    CommitTime = lists:foldl(fun({CT1, _ST}, Acc) ->
                        vectorclock:min([CT1, Acc])
                                             end, CT, vector_orddict:to_list(PrunedSnapshots)),
            {Key, Length, OpId, ListLen, OpsDict} = case ets:lookup(OpsCache, Key) of
                                                        [] ->
                                                            {Key, 0, 0, 0, []};
                                                        [Tuple] ->
                                                            tuple_to_key(Tuple)
                                                    end,
            {NewLength, PrunedOps} = prune_ops({Length, OpsDict}, CommitTime, Protocol),
            true = ets:insert(SnapshotCache, {Key, PrunedSnapshots}),
            %% Check if the pruned ops are lager or smaller than the previous list size
            %% if so create a larger or smaller list (by dividing or multiplying by 2)
            %% (Another option would be to shrink to a more "minimum" size, but need to test to see what is better)
            NewListLen = case NewLength > ListLen - ?RESIZE_THRESHOLD of
                             true ->
                                 ListLen * 2;
                             false ->
                                 HalfListLen = ListLen div 2,
                                 case HalfListLen =< ?OPS_THRESHOLD of
                                     true ->
                                         %% Don't shrink list, already minimun size
                                         ListLen;
                                     false ->
                                         %% Only shrink if shrinking would leave some space for new ops
                                         case HalfListLen - ?RESIZE_THRESHOLD > NewLength of
                                             true ->
                                                 HalfListLen;
                                             false ->
                                                 ListLen
                                         end
                                 end
                         end,
            true = ets:insert(OpsCache, erlang:make_tuple(?FIRST_OP+NewListLen,0,[{1,Key},{2,{NewLength,NewListLen}},{3,OpId}|PrunedOps]));
        false ->
            true = ets:insert(SnapshotCache, {Key, SnapshotDict})
    end.

%% @doc Remove from OpsDict all operations that have committed before Threshold.
-spec prune_ops({non_neg_integer(), [any(), ...]}, snapshot_time(), atom()) -> {non_neg_integer(), [any(), ...]}.
prune_ops({_Len, OpsDict}, Threshold, Protocol) ->
%% should write custom function for this in the vector_orddict
%% or have to just traverse the entire list?
%% since the list is ordered, can just stop when all values of
%% the op is smaller (i.e. not concurrent)
%% So can add a stop function to ordered_filter
%% Or can have the filter function return a tuple, one vale for stopping
%% one for including
    Res = reverse_and_filter(fun({_OpId, Op}) ->
        BaseSnapshotVC = case Protocol of {ok, physics} -> Op#operation_payload.dependency_vc;
                             _ -> Op#operation_payload.snapshot_vc
                         end,
        {DcId, CommitTime} = Op#operation_payload.dc_and_commit_time,
        CommitVC = vectorclock:create_commit_vector_clock(DcId, CommitTime, BaseSnapshotVC),
        (op_not_already_in_snapshot(Threshold, CommitVC))
                             end, lists:reverse(OpsDict), ?FIRST_OP, []),
    case Res of
        {_, []} ->
            [First | _Rest] = OpsDict,
            {1, [{?FIRST_OP, First}]};
        _ ->
            Res
    end.


%% This is an internal function used to convert the tuple stored in ets
%% to a tuple and list usable by the materializer
-spec tuple_to_key(tuple()) -> {any(), integer(), non_neg_integer(), non_neg_integer(), list()}.
tuple_to_key(Tuple) ->
    Key = element(1, Tuple),
    {Length, ListLen} = element(2, Tuple),
    OpId = element(3, Tuple),
    Ops = tuple_to_key_int(?FIRST_OP, Length + ?FIRST_OP, Tuple, []),
    {Key, Length, OpId, ListLen, Ops}.
tuple_to_key_int(Next, Next, _Tuple, Acc) ->
    Acc;
tuple_to_key_int(Next, Last, Tuple, Acc) ->
    tuple_to_key_int(Next + 1, Last, Tuple, [element(Next, Tuple) | Acc]).

%% This is an internal function used to filter ops and reverse the list
%% It returns a tuple where the first element is the lenght of the list returned
%% The elements in the list also include the location that they will be placed
%% in the tuple in the ets table, this way the list can be used
%% directly in the erlang:make_tuple function
-spec reverse_and_filter(fun(),list(),non_neg_integer(),list()) -> {non_neg_integer(),list()}.
reverse_and_filter(_Fun,[],Id,Acc) ->
    {Id-?FIRST_OP,Acc};
reverse_and_filter(Fun,[First|Rest],Id,Acc) ->
    case Fun(First) of
        true ->
            reverse_and_filter(Fun,Rest,Id+1,[{Id,First}|Acc]);
        false ->
            reverse_and_filter(Fun,Rest,Id,Acc)
    end.

%% @doc Insert an operation and start garbage collection triggered by writes.
%% the mechanism is very simple; when there are more than OPS_THRESHOLD
%% operations for a given key, just perform a read, that will trigger
%% the GC mechanism.
-spec op_insert_gc(key(), operation_payload(), cache_id(), cache_id(), transaction() | no_txn_inserting_from_log) -> ok | {error, {op_gc_error, any()}}.
op_insert_gc(Key, DownstreamOp, OpsCache, SnapshotCache, Transaction) ->
    case ets:member(OpsCache, Key) of
        false ->
            ets:insert(OpsCache, erlang:make_tuple(?FIRST_OP + ?OPS_THRESHOLD, 0, [{1, Key}, {2, {0, ?OPS_THRESHOLD}}]));
        true ->
            ok
    end,
    NewId = ets:update_counter(OpsCache, Key, {3, 1}),
    {Length, ListLen} = ets:lookup_element(OpsCache, Key, 2),
    %% Perform the GC in case the list is full, or every ?OPS_THRESHOLD operations (which ever comes first)
    case ((Length) >= ListLen) or ((NewId rem ?OPS_THRESHOLD) == 0) of
        true ->
            Type = DownstreamOp#operation_payload.type,
            NewTransaction = case Transaction of
                                 no_txn_inserting_from_log -> %% the function is being called by the logging vnode at startup
                                     {ok, Protocol} = application:get_env(antidote, txn_prot),
                                     #transaction{snapshot_vc = DownstreamOp#operation_payload.snapshot_vc,
                                         transactional_protocol = Protocol, txn_id = no_txn_inserting_from_log};
                                 _ ->
                                     Transaction#transaction{txn_id = no_txn_inserting_from_log,
                                         snapshot_vc = case Transaction#transaction.transactional_protocol of
                                                           physics ->
                                                               case DownstreamOp#operation_payload.dependency_vc of
                                                                   [] ->
                                                                       vectorclock:set_clock_of_dc(dc_utilities:get_my_dc_id(), clocksi_vnode:now_microsec(now()), []);
                                                                   DepVC -> DepVC
                                                               end;
                                                           Protocol when ((Protocol == gr) or (Protocol == clocksi)) ->
                                                               DownstreamOp#operation_payload.snapshot_vc
                                                       end}
                             end,
            case internal_read(Key, Type, NewTransaction, OpsCache, SnapshotCache, true) of
                {ok, _} ->
                    %% Have to get the new ops dict because the interal_read can change it
                    {Length1, ListLen1} = ets:lookup_element(OpsCache, Key, 2),
%%            lager:info("BEFORE GC: Key ~p,  Length ~p,  ListLen ~p",[Key, Length, ListLen]),
%%            lager:info("AFTER GC: Key ~p,  Length ~p,  ListLen ~p",[Key, Length1, ListLen1]),
                    true = ets:update_element(OpsCache, Key, [{Length1 + ?FIRST_OP, {NewId, DownstreamOp}}, {2, {Length1 + 1, ListLen1}}]),
                    ok;
                {error, Reason} ->
                    {error, {op_gc_error, Reason}}
            end;

        false ->
            true = ets:update_element(OpsCache, Key, [{Length + ?FIRST_OP, {NewId, DownstreamOp}}, {2, {Length + 1, ListLen}}]),
            ok
    end.

-ifdef(TEST).

%% @doc Testing belongs_to_snapshot returns true when a commit time
%% is smaller than a snapshot time
belongs_to_snapshot_test()->
    CommitTime1a= 1,
    CommitTime2a= 1,
    CommitTime1b= 1,
    CommitTime2b= 7,
    SnapshotClockDC1 = 5,
    SnapshotClockDC2 = 5,
    CommitTime3a= 5,
    CommitTime4a= 5,
    CommitTime3b= 10,
    CommitTime4b= 10,

    SnapshotVC=vectorclock:from_list([{1, SnapshotClockDC1}, {2, SnapshotClockDC2}]),
    ?assertEqual(true, op_not_already_in_snapshot(
        vectorclock:from_list([{1, CommitTime1a},{2,CommitTime1b}]),
        vectorclock:create_commit_vector_clock(1, SnapshotClockDC1, SnapshotVC))),
    ?assertEqual(true, op_not_already_in_snapshot(
        vectorclock:from_list([{1, CommitTime2a},{2,CommitTime2b}]),
        vectorclock:create_commit_vector_clock(2, SnapshotClockDC2, SnapshotVC))),
    ?assertEqual(false, op_not_already_in_snapshot(
        vectorclock:from_list([{1, CommitTime3a},{2,CommitTime3b}]),
        vectorclock:create_commit_vector_clock(1, SnapshotClockDC1, SnapshotVC))),
    ?assertEqual(false, op_not_already_in_snapshot(
        vectorclock:from_list([{1, CommitTime4a},{2,CommitTime4b}]),
        vectorclock:create_commit_vector_clock(2, SnapshotClockDC2, SnapshotVC))).

%% @doc This tests to make sure when garbage collection happens, no updates are lost
gc_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    DC1 = 1,
    Type = riak_dt_gcounter,

    %% Make 10 snapshots

    {ok, {Res0, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,2}])}, OpsCache, SnapshotCache),
    ?assertEqual(0, Type:value(Res0)),

    op_insert_gc(Key, generate_payload(10,11,Res0,a1), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res1, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,12}])}, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),

    op_insert_gc(Key, generate_payload(20,21,Res1,a2), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res2, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,22}])}, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)),

    op_insert_gc(Key, generate_payload(30,31,Res2,a3), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res3, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,32}])}, OpsCache, SnapshotCache),
    ?assertEqual(3, Type:value(Res3)),

    op_insert_gc(Key, generate_payload(40,41,Res3,a4), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res4, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,42}])}, OpsCache, SnapshotCache),
    ?assertEqual(4, Type:value(Res4)),

    op_insert_gc(Key, generate_payload(50,51,Res4,a5), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res5, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,52}])}, OpsCache, SnapshotCache),
    ?assertEqual(5, Type:value(Res5)),

    op_insert_gc(Key, generate_payload(60,61,Res5,a6), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res6, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,62}])}, OpsCache, SnapshotCache),
    ?assertEqual(6, Type:value(Res6)),

    op_insert_gc(Key, generate_payload(70,71,Res6,a7), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res7, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,72}])}, OpsCache, SnapshotCache),
    ?assertEqual(7, Type:value(Res7)),

    op_insert_gc(Key, generate_payload(80,81,Res7,a8), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res8, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,82}])}, OpsCache, SnapshotCache),
    ?assertEqual(8, Type:value(Res8)),

    op_insert_gc(Key, generate_payload(90,91,Res8,a9), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res9, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,92}])}, OpsCache, SnapshotCache),
    ?assertEqual(9, Type:value(Res9)),

    op_insert_gc(Key, generate_payload(100,101,Res9,a10), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),

    %% Insert some new values

    op_insert_gc(Key, generate_payload(15,111,Res1,a11), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    op_insert_gc(Key, generate_payload(16,121,Res1,a12), OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),

    %% Trigger the clean

    Tx = #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,102}])},

    {ok, {Res10, _}} = internal_read(Key, Type,
        Tx , OpsCache, SnapshotCache),
    ?assertEqual(10, Type:value(Res10)),

    op_insert_gc(Key, generate_payload(102,131,Res9,a13), OpsCache, SnapshotCache, Tx),

    %% Be sure you didn't loose any updates
    {ok, {Res13, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,142}])}, OpsCache, SnapshotCache),
    ?assertEqual(13, Type:value(Res13)).

%% @doc This tests to make sure operation lists can be large and resized
large_list_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    DC1 = 1,
    Type = riak_dt_gcounter,

    %% Make 1000 updates to grow the list, whithout generating a snapshot to perform the gc
    {ok, {Res0, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,2}])}, OpsCache, SnapshotCache),
    ?assertEqual(0, Type:value(Res0)),
%%    lager:info("Res0 = ~p", [Res0]),

    lists:foreach(fun(Val) ->
        Op = generate_payload(10,11+Val,Res0,Val),
%%        lager:info("Op= ~p", [Op]),
        op_insert_gc(Key, Op, OpsCache, SnapshotCache,
            #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{1,11+Val}])} )
                  end, lists:seq(1,1000)),

    {ok, {Res1000, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,2000}])}, OpsCache, SnapshotCache),
    ?assertEqual(1000, Type:value(Res1000)),

    %% Now check everything is ok as the list shrinks from generating new snapshots
    lists:foreach(fun(Val) ->
        op_insert_gc(Key, generate_payload(10+Val,11+Val,Res0,Val), OpsCache, SnapshotCache,
            #transaction{txn_id = eunit_test, transactional_protocol = clocksi,
                snapshot_vc = vectorclock:from_list([{DC1,2000}])}),
        {ok, {Res, _}} = internal_read(Key, Type,
            #transaction{txn_id = eunit_test, transactional_protocol = clocksi,
                snapshot_vc = vectorclock:from_list([{DC1,2000}])}, OpsCache, SnapshotCache),
        ?assertEqual(Val, Type:value(Res))
                  end, lists:seq(1001,1100)).

generate_payload(SnapshotTime,CommitTime,Prev,Name) ->
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 1,

    {ok,Op1} = Type:update(increment, Name, Prev),
    #operation_payload{key = Key,
        type = Type,
        op_param = {merge, Op1},
        snapshot_vc = vectorclock:from_list([{DC1,SnapshotTime}]),
        dc_and_commit_time = {DC1,CommitTime},
        txid = 1
    }.

seq_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 1,
    S1 = Type:new(),

    %% Insert one increment
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #operation_payload{key = Key,
        type = Type,
        op_param = {merge, Op1},
        snapshot_vc = vectorclock:from_list([{DC1,10}]),
        dc_and_commit_time = {DC1, 15},
        txid = 1
    },
    op_insert_gc(Key,DownstreamOp1, OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res1, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1, 16}])}, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),
    %% Insert second increment
    {ok,Op2} = Type:update(increment, a, Res1),
    DownstreamOp2 = DownstreamOp1#operation_payload{
        op_param = {merge, Op2},
        snapshot_vc =vectorclock:from_list([{DC1,16}]),
        dc_and_commit_time = {DC1,20},
        txid=2},

    op_insert_gc(Key,DownstreamOp2, OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res2, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1, 21}])}, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, {ReadOld, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1, 16}])}, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadOld)).

multipledc_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = 1,
    DC2 = 2,
    S1 = Type:new(),

    %% Insert one increment in DC1
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #operation_payload{key = Key,
        type = Type,
        op_param = {merge, Op1},
        snapshot_vc = vectorclock:from_list([{DC2,0}, {DC1,10}]),
        dc_and_commit_time = {DC1, 15},
        txid = 1
    },
    op_insert_gc(Key,DownstreamOp1,OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res1, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1, 16}, {DC2, 0}])}, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),

    %% Insert second increment in other DC
    {ok,Op2} = Type:update(increment, b, Res1),
    DownstreamOp2 = DownstreamOp1#operation_payload{
        op_param = {merge, Op2},
        snapshot_vc =vectorclock:from_list([{DC2,16}, {DC1,16}]),
        dc_and_commit_time = {DC2,20},
        txid=2},

    op_insert_gc(Key,DownstreamOp2,OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res2, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,16}, {DC2,21}])}, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)),

    %% Read old version
    {ok, {ReadOld, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,15}, {DC2,15}])}, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadOld)).

concurrent_write_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Key = mycount,
    Type = riak_dt_gcounter,
    DC1 = local,
    DC2 = remote,
    S1 = Type:new(),

    %% Insert one increment in DC1
    {ok,Op1} = Type:update(increment, a, S1),
    DownstreamOp1 = #operation_payload{key = Key,
        type = Type,
        op_param = {merge, Op1},
        snapshot_vc = vectorclock:from_list([{DC1,0}, {DC2,0}]),
        dc_and_commit_time = {DC2, 1},
        txid = 1
    },
    op_insert_gc(Key,DownstreamOp1,OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),
    {ok, {Res1, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,0}, {DC2,1}])}, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(Res1)),

    %% Another concurrent increment in other DC
    {ok, Op2} = Type:update(increment, b, S1),
    DownstreamOp2 = #operation_payload{ key = Key,
        type = Type,
        op_param = {merge, Op2},
        snapshot_vc =vectorclock:from_list([{DC1,0}, {DC2,0}]),
        dc_and_commit_time = {DC1, 1},
        txid=2},
    op_insert_gc(Key,DownstreamOp2,OpsCache, SnapshotCache, #transaction{txn_id = eunit_test}),

    %% Read different snapshots
    {ok, {ReadDC1, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,1}, {DC2,0}])}, OpsCache, SnapshotCache),
    ?assertEqual(1, Type:value(ReadDC1)),
    io:format("Result1 = ~p", [ReadDC1]),
    {ok, {ReadDC2, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,0}, {DC2,1}])}, OpsCache, SnapshotCache),
    io:format("Result2 = ~p", [ReadDC2]),
    ?assertEqual(1, Type:value(ReadDC2)),

    %% Read snapshot including both increments
    {ok, {Res2, _}} = internal_read(Key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{DC1,1}, {DC2,1}])}, OpsCache, SnapshotCache),
    ?assertEqual(2, Type:value(Res2)).

%% Check that a read to a key that has never been read or updated, returns the CRDTs initial value
%% E.g., for a gcounter, return 0.
read_nonexisting_key_test() ->
    OpsCache = ets:new(ops_cache, [set]),
    SnapshotCache = ets:new(snapshot_cache, [set]),
    Type = riak_dt_gcounter,
    {ok, {ReadResult, _}} = internal_read(key, Type,
        #transaction{txn_id = eunit_test, transactional_protocol = clocksi, snapshot_vc = vectorclock:from_list([{dc1,1}, {dc2,0}])}, OpsCache, SnapshotCache),
    ?assertEqual(0, Type:value(ReadResult)).

-endif.