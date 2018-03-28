/*
 * Tencent is pleased to support the open source community by making Angel available.
 * 
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 * 
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * 
 * https://opensource.org/licenses/BSD-3-Clause
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.VoidResult;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.psagent.matrix.cache.MatricesCache;
import com.tencent.angel.psagent.matrix.oplog.cache.MatrixOpLog;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import com.tencent.angel.psagent.matrix.storage.MatrixStorage;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportClient;
import com.tencent.angel.psagent.task.TaskContext;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The adapter between user requests and actual rpc requests. Because a matrix is generally
 * distributed in multiple parameter servers, so an application request generally corresponds to
 * multiple rpc requests. The adapter can split the application request to sub-requests(rpc
 * requests) and merge the results of them, then return the final result.
 */
public class MatrixClientAdapter {
  private static final Log LOG = LogFactory.getLog(MatrixClientAdapter.class);
  /**
   * matrix id to the lock for GET_ROWS request map
   */
  private final ConcurrentHashMap<Integer, ReentrantLock> locks;

  /**
   * result cache for GET_ROWS requests
   */
  private final ConcurrentHashMap<RowIndex, GetRowsResult> resultsMap;

  /**
   * matrix id to fetching rows indexes map, use to distinct requests to same rows
   */
  private final ConcurrentHashMap<Integer, IntOpenHashSet> fetchingRowSets;

  /**
   * matrix id -> (row index -> the number of row splits the row contains), se to determine whether
   * all splits for a row are all fetched
   */
  private final Map<Integer, Int2IntOpenHashMap> matrixToRowSplitSizeCache;

  /**
   * user request to the sub-request results cache map
   */
  private final ConcurrentHashMap<UserRequest, PartitionResponseCache> requestToResponseMap;

  /**
   * client worker pool: 1.use to deserialize partition responses and merge them to final result
   * 2.use to generate partition request and serialize it
   */
  private final ExecutorService workerPool;

  /**
   * the sub-request results merge dispatcher
   */
  private Thread mergeDispatcher;

  /**
   * stop the merge dispatcher and all workers
   */
  private final AtomicBoolean stopped;

  /**
   * update clock to master use sync mode
   */
  private final boolean syncClockEnable;

  /**
   * Create a new MatrixClientAdapter.
   */
  public MatrixClientAdapter() {
    locks = new ConcurrentHashMap<Integer, ReentrantLock>();
    resultsMap = new ConcurrentHashMap<RowIndex, GetRowsResult>();
    fetchingRowSets = new ConcurrentHashMap<Integer, IntOpenHashSet>();
    matrixToRowSplitSizeCache = new HashMap<Integer, Int2IntOpenHashMap>();
    requestToResponseMap = new ConcurrentHashMap<UserRequest, PartitionResponseCache>();
    workerPool = Executors.newCachedThreadPool();
    stopped = new AtomicBoolean(false);
    syncClockEnable = PSAgentContext.get().syncClockEnable();
  }

  /**
   * Start the sub-request results merge dispatcher.
   */
  public void start() {
    mergeDispatcher = new MergeDispatcher();
    mergeDispatcher.setName("row-merge-dispatcher");
    mergeDispatcher.start();
  }

  /**
   * Stop the merge dispatcher and all workers.
   */
  public void stop() {
    if (!stopped.getAndSet(true)) {
      workerPool.shutdownNow();
      if (mergeDispatcher != null) {
        mergeDispatcher.interrupt();
        mergeDispatcher = null;
      }
    }
  }

  /**
   * Get a matrix row from parameter servers
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param clock    clock value
   * @return TVector matrix row
   * @throws ExecutionException   exception thrown when attempting to retrieve the result of a task
   *                              that aborted by throwing an exception
   * @throws InterruptedException interrupted while wait the result
   */
  public TVector getRow(int matrixId, int rowIndex, int clock)
    throws InterruptedException, ExecutionException {
    LOG.debug("start to getRow request, matrix=" + matrixId + ", rowIndex=" + rowIndex + ", clock="
      + clock);
    long startTs = System.currentTimeMillis();
    // Wait until the clock value of this row is greater than or equal to the value
    PSAgentContext.get().getConsistencyController().waitForClock(matrixId, rowIndex, clock);
    LOG.debug("getRow wait clock time=" + (System.currentTimeMillis() - startTs));

    startTs = System.currentTimeMillis();
    // Get partitions for this row
    List<PartitionKey> partList =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowIndex);
    GetRowRequest request = new GetRowRequest(matrixId, rowIndex, clock);
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);

    GetRowPipelineCache responseCache = (GetRowPipelineCache) requestToResponseMap.get(request);
    if (responseCache == null) {
      responseCache = new GetRowPipelineCache(partList.size(), meta.getRowType());
      GetRowPipelineCache oldCache =
        (GetRowPipelineCache) requestToResponseMap.putIfAbsent(request, responseCache);
      if (oldCache != null) {
        responseCache = oldCache;
      }
    }

    // First get this row from matrix storage
    MatrixStorage matrixStorage =
      PSAgentContext.get().getMatrixStorageManager().getMatrixStoage(matrixId);
    try {
      responseCache.getDistinctLock().lock();

      // If the row exists in the matrix storage and the clock value meets the requirements, just
      // return
      TVector row = matrixStorage.getRow(rowIndex);
      if (row != null && row.getClock() >= clock) {
        return row;
      }

      // Get row splits of this row from the matrix cache first
      MatricesCache matricesCache = PSAgentContext.get().getMatricesCache();
      MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
      int size = partList.size();
      for (int i = 0; i < size; i++) {
        ServerRow rowSplit = matricesCache.getRowSplit(matrixId, partList.get(i), rowIndex);
        if (rowSplit != null && rowSplit.getClock() >= clock) {
          responseCache.addRowSplit(rowSplit);
        } else {
          // If the row split does not exist in cache, get it from parameter server
          responseCache.addRowSplit(matrixClient.getRowSplit(partList.get(i), rowIndex, clock));
        }
      }

      // Wait the final result
      row = responseCache.getMergedResult().get();
      LOG.debug("get row use time=" + (System.currentTimeMillis() - startTs));
      // Put it to the matrix cache
      matrixStorage.addRow(rowIndex, row);
      return row;
    } finally {
      responseCache.getDistinctLock().unlock();
      requestToResponseMap.remove(request);
    }
  }

  /**
   * Update matrix use a udf.
   *
   * @param updateFunc update udf function
   * @return Future<VoidResult> update future result
   */
  public Future<VoidResult> update(UpdateFunc updateFunc) {
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    UpdateParam param = updateFunc.getParam();

    List<PartitionUpdateParam> partParams = param.split();

    int size = partParams.size();
    UpdaterRequest request = new UpdaterRequest(param);
    UpdaterResponseCache cache = new UpdaterResponseCache(size);
    for (int i = 0; i < size; i++) {
      cache.addResult(matrixClient.update(updateFunc, partParams.get(i)));
    }

    requestToResponseMap.put(request, cache);
    return cache.getMergedResult();
  }

  /**
   * Flush the matrix oplog to parameter servers.
   *
   * @param matrixId    matrix id
   * @param taskContext task context
   * @param matrixOpLog matrix oplog
   * @param updateClock true means we should update the clock value after update matrix
   * @return Future<VoidResult> flush future result
   */
  public Future<VoidResult> flush(int matrixId, TaskContext taskContext, MatrixOpLog matrixOpLog,
    boolean updateClock) {
    if (!updateClock && (matrixOpLog == null)) {
      FutureResult<VoidResult> ret = new FutureResult<VoidResult>();
      ret.set(new VoidResult(ResponseType.SUCCESS));
      return ret;
    }

    Map<PartitionKey, List<RowUpdateSplit>> psUpdateData =
      new HashMap<PartitionKey, List<RowUpdateSplit>>();
    FlushRequest request =
      new FlushRequest(taskContext.getMatrixClock(matrixId), taskContext.getIndex(), matrixId,
        matrixOpLog, updateClock);

    long startTs = System.currentTimeMillis();
    // Split the matrix oplog according to the matrix partitions
    if (matrixOpLog != null) {
      matrixOpLog.split(psUpdateData);
    }
    LOG.debug("split use time=" + (System.currentTimeMillis() - startTs));

    // If need update clock, we should send requests to all partitions
    if (updateClock) {
      fillPartRequestForClock(matrixId, psUpdateData, taskContext);
    }

    FlushResponseCache cache = new FlushResponseCache(psUpdateData.size());
    pushUpdates(matrixId, psUpdateData, taskContext, updateClock, cache);
    requestToResponseMap.put(request, cache);
    return cache.getMergedResult();
  }

  private void fillPartRequestForClock(int matrixId,
    Map<PartitionKey, List<RowUpdateSplit>> psUpdateData, TaskContext taskContext) {
    List<PartitionKey> partitions = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = partitions.size();
    for (int i = 0; i < size; i++) {
      if (!psUpdateData.containsKey(partitions.get(i))) {
        psUpdateData.put(partitions.get(i), new ArrayList<RowUpdateSplit>());
      }
    }
  }

  private void pushUpdates(int matrixId, Map<PartitionKey, List<RowUpdateSplit>> psUpdateData,
    TaskContext taskContext, boolean updateClock, FlushResponseCache cache) {
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();

    for (Entry<PartitionKey, List<RowUpdateSplit>> partUpdateEntry : psUpdateData.entrySet()) {
      cache.addResult(matrixClient
        .putPart(partUpdateEntry.getKey(), partUpdateEntry.getValue(), taskContext.getIndex(),
          taskContext.getMatrixClock(matrixId), updateClock));
    }
  }

  /**
   * Get rows use pipeline mode.
   *
   * @param result       result cache
   * @param rowIndex     the indexes of rows that need to fetch from ps
   * @param rpcBatchSize how many rows to be fetched in a rpc
   * @param clock        clock value
   * @return result cache
   */
  public GetRowsResult getRowsFlow(GetRowsResult result, RowIndex rowIndex, int rpcBatchSize,
    int clock) {
    LOG.debug("get rows request, rowIndex=" + rowIndex);
    if (rpcBatchSize == -1) {
      rpcBatchSize = chooseRpcBatchSize(rowIndex);
    }

    // Filter the rowIds which are fetching now
    ReentrantLock lock = getLock(rowIndex.getMatrixId());
    RowIndex needFetchRows = null;
    try {
      lock.lock();
      resultsMap.put(rowIndex, result);

      if (!fetchingRowSets.containsKey(rowIndex.getMatrixId())) {
        fetchingRowSets.put(rowIndex.getMatrixId(), new IntOpenHashSet());
      }

      if (!matrixToRowSplitSizeCache.containsKey(rowIndex.getMatrixId())) {
        matrixToRowSplitSizeCache.put(rowIndex.getMatrixId(), new Int2IntOpenHashMap());
      }

      needFetchRows = findNewRows(rowIndex);
    } finally {
      lock.unlock();
    }

    // Send the rowIndex to rpc dispatcher and return immediately
    if (needFetchRows.getRowsNumber() > 0) {
      dispatchGetRows(needFetchRows, rpcBatchSize, clock);
    }
    return resultsMap.get(rowIndex);
  }

  /**
   * Get a row from ps use a udf.
   *
   * @param func get row udf
   * @return GetResult the result of the udf
   * @throws ExecutionException   exception thrown when attempting to retrieve the result of a task
   *                              that aborted by throwing an exception
   * @throws InterruptedException interrupted while wait the result
   */
  public GetResult get(GetFunc func) throws InterruptedException, ExecutionException {
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    GetParam param = func.getParam();
    List<PartitionGetParam> partParams = param.split();
    int size = partParams.size();

    List<Future<PartitionGetResult>> futureResultList =
      new ArrayList<Future<PartitionGetResult>>(size);
    List<PartitionGetResult> resultList = new ArrayList<PartitionGetResult>(size);

    for (int i = 0; i < size; i++) {
      futureResultList.add(matrixClient.get(func, partParams.get(i)));
    }

    for (int i = 0; i < size; i++) {
      resultList.add(futureResultList.get(i).get());
    }

    return func.merge(resultList);
  }

  /**
   * Sub-request results merge dispatcher.
   */
  class MergeDispatcher extends Thread {
    @Override public void run() {
      int checkTime = 0;
      try {
        while (!stopped.get() && !Thread.interrupted()) {
          Iterator<Entry<UserRequest, PartitionResponseCache>> iter =
            requestToResponseMap.entrySet().iterator();
          while (iter.hasNext()) {
            Entry<UserRequest, PartitionResponseCache> entry = iter.next();

            if (LOG.isDebugEnabled()) {
              if (checkTime % 100 == 0) {
                LOG.debug(
                  "waiting user request=" + entry.getKey() + ", result cache=" + entry.getValue());
              }
            }

            switch (entry.getKey().type) {
              case GET_ROW: {
                GetRowPipelineCache cache = (GetRowPipelineCache) entry.getValue();
                if (cache.canStartMerge() && !cache.getIsMerging()) {
                  cache.setIsMerging(true);
                  workerPool.execute(new RowMerger((GetRowRequest) entry.getKey(), cache));
                }
                break;
              }

              case GET_ROWS: {
                GetRowsFlowCache cache = (GetRowsFlowCache) entry.getValue();
                Int2ObjectOpenHashMap<List<ServerRow>> rows = cache.getNeedMergeRows();
                if (rows != null && !rows.isEmpty()) {
                  workerPool.execute(new RowsFlowMerger((GetRowsFlowRequest) entry.getKey(), rows));
                }

                if (cache.isReceivedOver()) {
                  iter.remove();
                }
                break;
              }

              case UPDATER: {
                UpdaterResponseCache cache = (UpdaterResponseCache) entry.getValue();
                cache.checkFutures();
                if (cache.isReceivedOver()) {
                  cache.setIsMerging(true);
                  VoidResult result = mergeUpdaterResult(cache.getResultList());
                  cache.setMergedResult(result);
                  iter.remove();
                }
                break;
              }

              case FLUSH: {
                FlushResponseCache cache = (FlushResponseCache) entry.getValue();
                cache.checkFutures();
                if (cache.isReceivedOver()) {
                  FlushRequest request = (FlushRequest) entry.getKey();
                  if (request.isUpdateClock() && syncClockEnable) {
                    PSAgentContext.get().getMasterClient()
                      .updateClock(request.getTaskIndex(), request.getMatrixId(),
                        request.getClock());
                  }
                  cache.setIsMerging(true);
                  VoidResult result = mergeFlushResult(cache.getResultList());
                  cache.setMergedResult(result);
                  iter.remove();
                }
                break;
              }

              default:
                break;
            }
          }

          Thread.sleep(10);
          checkTime++;
        }
      } catch (InterruptedException ie) {
        if(stopped.get() || Thread.interrupted()) {
          LOG.info("MergeDispatcher is interrupted");
        }
      } catch (Exception e) {
        LOG.fatal("merge dispatcher error ", e);
        PSAgentContext.get().getPsAgent().error("merge dispatcher error " + e.getMessage());
      }
    }
  }

  private void clean() {
    Iterator<Entry<UserRequest, PartitionResponseCache>> iter =
      requestToResponseMap.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<UserRequest, PartitionResponseCache> entry = iter.next();

    }
  }


  /**
   * Row splits merge thread.
   */
  class RowMerger extends Thread {
    private final GetRowRequest request;
    private final PartitionResponseCache cache;

    public RowMerger(GetRowRequest request, PartitionResponseCache cache) {
      this.request = request;
      this.cache = cache;
    }

    private void mergeRowPipeline(GetRowPipelineCache pipelineCache) {
      try {
        TVector vector = RowSplitCombineUtils
          .combineRowSplitsPipeline(pipelineCache, request.getMatrixId(), request.getRowIndex());
        vector.setMatrixId(request.getMatrixId());
        pipelineCache.setMergedResult(vector);
      } catch (Exception x) {
        LOG.fatal("merge row failed ", x);
        PSAgentContext.get().getPsAgent().error("merge row splits failed " + x.getMessage());
      }
    }

    @Override public void run() {
      if (cache instanceof GetRowPipelineCache) {
        mergeRowPipeline((GetRowPipelineCache) cache);
      }
    }
  }


  /**
   * Merge thread for GET_ROWS request.
   */
  public class RowsFlowMerger implements Runnable {
    private final GetRowsFlowRequest request;
    private final Int2ObjectOpenHashMap<List<ServerRow>> rowSplits;

    public RowsFlowMerger(GetRowsFlowRequest request,
      Int2ObjectOpenHashMap<List<ServerRow>> needMergeList) {
      this.request = request;
      this.rowSplits = needMergeList;
    }

    @Override public void run() {
      for (Entry<Integer, List<ServerRow>> entry : rowSplits.entrySet()) {
        notifyAllGetRows(mergeSplit(entry.getKey(), entry.getValue()));
      }
    }

    private TVector mergeSplit(int rowIndex, List<ServerRow> splits) {
      TVector vector = null;
      try {
        vector = RowSplitCombineUtils
          .combineServerRowSplits(splits, request.getIndex().getMatrixId(), rowIndex);
        return vector;
      } catch (Exception x) {
        LOG.fatal("merge row failed ", x);
        PSAgentContext.get().getPsAgent().error("merge row splits failed " + x.getMessage());
      }

      return vector;
    }

    private void notifyAllGetRows(TVector row) {
      if(row == null) {
        return;
      }
      PSAgentContext.get().getMatrixStorageManager().addRow(row.getMatrixId(), row.getRowId(), row);
      ReentrantLock lock = getLock(row.getMatrixId());
      try {
        lock.lock();

        Iterator<Entry<RowIndex, GetRowsResult>> iter = resultsMap.entrySet().iterator();
        Entry<RowIndex, GetRowsResult> resultEntry = null;
        while (iter.hasNext()) {
          resultEntry = iter.next();
          if (resultEntry.getKey().getMatrixId() == row.getMatrixId() && resultEntry.getKey()
            .contains(row.getRowId()) && !resultEntry.getKey().isFilted(row.getRowId())) {
            resultEntry.getKey().filted(row.getRowId());
            resultEntry.getValue().put(row);
          }

          if (resultEntry.getKey().getRowsNumber() == resultEntry.getValue().getRowsNumber()) {
            resultEntry.getKey().clearFilted();
            resultEntry.getValue().fetchOver();
            iter.remove();
          }
        }

        IntOpenHashSet fetchingRowsForMatrix = fetchingRowSets.get(row.getMatrixId());
        if (fetchingRowsForMatrix != null) {
          fetchingRowsForMatrix.remove(row.getRowId());
        }
      } catch (InterruptedException e) {
        LOG.error("Interrupted when notify getrowrequest, exit now ", e);
      } finally {
        lock.unlock();
      }
    }
  }


  private ReentrantLock getLock(int matrixId) {
    if (!locks.containsKey(matrixId)) {
      locks.putIfAbsent(matrixId, new ReentrantLock());
    }
    return locks.get(matrixId);
  }

  public VoidResult mergeFlushResult(List<VoidResult> resultList) {
    return new VoidResult(ResponseType.SUCCESS);
  }

  public VoidResult mergeUpdaterResult(List<VoidResult> resultList) {
    return new VoidResult(ResponseType.SUCCESS);
  }

  /**
   * Split the rowIndex to batches to generate rpc dispatcher items
   *
   * @param rowIndex rowIds needed to been requested from PS
   */
  private void dispatchGetRows(RowIndex rowIndex, int rpcBatchSize, int clock) {
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();

    // Get the partition to sub-row splits map:use to storage the rows stored in a matrix partition
    Map<PartitionKey, List<RowIndex>> partToRowIndexMap =
      PSAgentContext.get().getMatrixMetaManager().getPartitionToRowIndexMap(rowIndex, rpcBatchSize);
    List<RowIndex> rowIds;
    int size;

    // Generate dispatch items and add them to the corresponding queues
    int totalRequestNumber = 0;
    for (Entry<PartitionKey, List<RowIndex>> entry : partToRowIndexMap.entrySet()) {
      totalRequestNumber += entry.getValue().size();
    }

    GetRowsFlowRequest request = new GetRowsFlowRequest(rowIndex, clock);

    // Filter the rowIds which are fetching now
    ReentrantLock lock = getLock(rowIndex.getMatrixId());
    Int2IntOpenHashMap rowIndexToPartSizeMap;
    try {
      lock.lock();
      rowIndexToPartSizeMap = matrixToRowSplitSizeCache.get(rowIndex.getMatrixId());
    } finally {
      lock.unlock();
    }

    GetRowsFlowCache cache = new GetRowsFlowCache(totalRequestNumber, rowIndex.getMatrixId(),
      rowIndexToPartSizeMap);

    for (Entry<PartitionKey, List<RowIndex>> entry : partToRowIndexMap.entrySet()) {
      totalRequestNumber += entry.getValue().size();
      rowIds = entry.getValue();
      size = rowIds.size();

      for (int i = 0; i < size; i++) {
        cache.addResult(
          matrixClient.getRowsSplit(entry.getKey(), rowIndexToList(rowIds.get(i)), clock));
      }
    }

    requestToResponseMap.put(request, cache);
  }

  private List<Integer> rowIndexToList(RowIndex index) {
    int[] rowIndexes = index.getRowIds().toIntArray();
    List<Integer> ret = new ArrayList<Integer>();
    for (int i = 0; i < rowIndexes.length; i++) {
      ret.add(rowIndexes[i]);
    }
    return ret;
  }

  private RowIndex findNewRows(RowIndex rowIndex) {
    IntOpenHashSet need = new IntOpenHashSet();
    IntOpenHashSet fetchingRowIds = fetchingRowSets.get(rowIndex.getMatrixId());

    IntIterator iter = rowIndex.getRowIds().iterator();
    while (iter.hasNext()) {
      int rowId = iter.nextInt();
      if (!fetchingRowIds.contains(rowId)) {
        need.add(rowId);
        fetchingRowIds.add(rowId);
      }
    }

    return new RowIndex(rowIndex.getMatrixId(), need, rowIndex);
  }

  private int chooseRpcBatchSize(RowIndex rowIndex) {
    PartitionKey part =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(rowIndex.getMatrixId()).get(0);
    int rowNumInPart = part.getEndRow() - part.getStartRow();
    return Math.max(rowNumInPart / 4, 10);
  }
}
