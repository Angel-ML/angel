/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.master.deploy.DeployUtils;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.ps.EmbeddedParameterServer;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.TransportMethod;
import com.tencent.angel.ps.server.data.request.GetRowSplitRequest;
import com.tencent.angel.ps.server.data.request.GetRowsSplitRequest;
import com.tencent.angel.ps.server.data.request.GetUDFRequest;
import com.tencent.angel.ps.server.data.request.IndexPartGetRowRequest;
import com.tencent.angel.ps.server.data.request.IndexPartGetRowsRequest;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.server.data.request.UpdateRequest;
import com.tencent.angel.ps.server.data.request.UpdateUDFRequest;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.LocalTransportClient;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportInterface;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportClient;
import com.tencent.angel.psagent.matrix.transport.response.MapResponseCache;
import com.tencent.angel.psagent.matrix.transport.response.ResponseCache;
import com.tencent.angel.psagent.matrix.transport.router.CompStreamKeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.DataPart;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The adapter between user requests and actual rpc requests. Because a matrix is generally
 * distributed in multiple parameter servers, so an application request generally corresponds to
 * multiple rpc requests. The adapter can split the application request to sub-requests(rpc
 * requests) and merge the results of them, then return the final result.
 */
public class UserRequestAdapter {

  private static final Log LOG = LogFactory.getLog(UserRequestAdapter.class);

  /**
   * Request id to request map. Registered FIRST so that a fast in-process response
   * (e.g. {@link com.tencent.angel.psagent.matrix.transport.LocalTransportClient}) cannot
   * observe a partially-registered state.
   */
  private final ConcurrentHashMap<Integer, UserRequest> requests;

  /**
   * Response cache. Registered SECOND.
   */
  private final ConcurrentHashMap<Integer, ResponseCache> requestIdToResponseCache;

  /**
   * Request id to result map. Registered LAST and used as the readiness gate on the response
   * path; once a thread sees a non-null FutureResult here, the other two maps are guaranteed
   * to also contain the entry (happens-before via {@link ConcurrentHashMap#put}).
   */
  private final ConcurrentHashMap<Integer, FutureResult> requestIdToResultMap;


  /**
   * Stop flag. Once flipped to {@code true} all new submissions will fail fast and any future
   * already in flight is terminated with an exception.
   */
  private final AtomicBoolean stopped;

  private final AngelDeployMode deployMode;

  /**
   * Background scheduler for the deadline watchdog (see {@link #checkDeadlines()}). A single
   * daemon thread is sufficient because the iteration is bounded by
   * {@code requests.size()} and each per-entry cost is constant. Lazily created in
   * {@link #start()} when the deadline is enabled, kept null otherwise so the no-deadline
   * deployment pays absolutely no overhead.
   */
  private volatile ScheduledExecutorService deadlineScheduler;

  /** Handle to the periodic deadline-check task; cancelled in {@link #stop()}. */
  private volatile ScheduledFuture<?> deadlineWatchdog;

  /**
   * Effective deadline (ms) read once in {@link #start()}. Zero or negative disables the
   * watchdog entirely (for backwards compat / latency-sensitive deployments).
   */
  private volatile long userRequestDeadlineMs;

  /**
   * Aggregate count of user requests that have been failed by the deadline watchdog since
   * adapter start. Surfaced for monitoring; high values point to a real PS-side stall and
   * the watchdog is doing its job (rather than over-reacting).
   */
  private final AtomicLong watchdogFailedCount = new AtomicLong(0);

  /**
   * Create a new UserRequestAdapter.
   */
  public UserRequestAdapter() {
    requests = new ConcurrentHashMap<>();
    requestIdToResponseCache = new ConcurrentHashMap<>();
    requestIdToResultMap = new ConcurrentHashMap<>();

    stopped = new AtomicBoolean(false);

    // Get deploy mode
    deployMode = DeployUtils.getDeployMode(PSAgentContext.get().getConf());
  }

  /**
   * Start the adapter. Wires up the {@code deadline watchdog} when enabled by configuration
   * (see {@link AngelConf#ANGEL_PSAGENT_USER_REQUEST_DEADLINE_MS}). The watchdog is the
   * last-resort safety net that prevents a single stuck partition response from hanging
   * Spark tasks indefinitely on {@code FutureResult.get()}.
   */
  public void start() {
    org.apache.hadoop.conf.Configuration conf = PSAgentContext.get().getConf();
    long deadline = conf.getLong(AngelConf.ANGEL_PSAGENT_USER_REQUEST_DEADLINE_MS,
        AngelConf.DEFAULT_ANGEL_PSAGENT_USER_REQUEST_DEADLINE_MS);
    long interval = conf.getLong(AngelConf.ANGEL_PSAGENT_USER_REQUEST_DEADLINE_CHECK_INTERVAL_MS,
        AngelConf.DEFAULT_ANGEL_PSAGENT_USER_REQUEST_DEADLINE_CHECK_INTERVAL_MS);
    if (deadline <= 0) {
      LOG.info("UserRequestAdapter deadline watchdog disabled "
          + "(angel.psagent.user.request.deadline.ms <= 0)");
      this.userRequestDeadlineMs = 0L;
      return;
    }
    if (interval <= 0 || interval >= deadline) {
      // Make the cadence at most 1/4 of the deadline so the actual fail-fast latency is
      // tightly bounded by deadline (not 2x deadline due to a too-coarse tick).
      interval = Math.max(1000L, deadline / 4L);
      LOG.warn("UserRequestAdapter deadline check interval was invalid; "
          + "auto-adjusted to " + interval + "ms");
    }
    this.userRequestDeadlineMs = deadline;

    deadlineScheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
      @Override public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "user-request-adapter-watchdog");
        t.setDaemon(true);
        return t;
      }
    });
    final long deadlineMs = this.userRequestDeadlineMs;
    deadlineWatchdog = deadlineScheduler.scheduleWithFixedDelay(new Runnable() {
      @Override public void run() {
        if (!stopped.get()) {
          try {
            checkDeadlines();
          } catch (Throwable t) {
            // Never let a check exception kill the schedule; ScheduledExecutorService
            // would otherwise silently stop re-scheduling.
            LOG.error("UserRequestAdapter deadline watchdog tick failed", t);
          }
        }
      }
    }, interval, interval, TimeUnit.MILLISECONDS);

    LOG.info("UserRequestAdapter deadline watchdog enabled: deadline=" + deadlineMs
        + "ms, check-interval=" + interval + "ms");
  }

  /**
   * One iteration of the deadline watchdog. For every user request whose age exceeds the
   * configured deadline, flag it as failed via the same code path as a legitimate
   * permanent failure: {@link #requestError}. The future inside {@code FutureResult.get()}
   * wakes up immediately afterwards with a deterministic
   * {@link ExecutionException}, so the caller — typically a Spark executor thread — can
   * fail-and-retry instead of hanging the entire stage.
   *
   * <p>The watchdog reads from {@link #requests} (not {@code requestIdToResultMap})
   * because that map carries the {@code createTs} we need; the readiness gate on the
   * response side is unaffected.
   */
  void checkDeadlines() {
    long deadline = userRequestDeadlineMs;
    if (deadline <= 0 || requests.isEmpty()) {
      return;
    }
    long now = System.currentTimeMillis();
    java.util.List<Integer> expired =
        findExpiredRequestIds(requests, deadline, now);
    if (expired.isEmpty()) {
      return;
    }
    for (Integer id : expired) {
      UserRequest req = requests.get(id);
      if (req == null) {
        continue; // raced with normal completion
      }
      long age = now - req.getCreateTs();
      // Fail-fast via the standard error path so all three bookkeeping maps are evicted
      // and the caller's future wakes with a non-cryptic message including request id /
      // type / age. Using requestError keeps a single funnel for "external failure".
      String msg = "Adapter deadline exceeded: requestId=" + id
          + ", type=" + req.getType()
          + ", age=" + age + "ms (>" + deadline + "ms)."
          + " The request was force-failed by the user-request deadline watchdog;"
          + " a partition response is likely permanently stuck.";
      LOG.error(msg);
      requestError(id, msg);
    }
    long total = watchdogFailedCount.addAndGet(expired.size());
    LOG.warn("Deadline watchdog failed " + expired.size() + " request(s) this tick"
        + " (cumulative=" + total + ")");
  }

  /**
   * Pure function: scan the given request map and return the ids of every entry whose
   * {@code (now - createTs) > deadlineMs}. Hoisted out as a package-private static helper
   * so that the deadline-detection contract can be unit-tested without standing up an
   * adapter, scheduler, or PSAgent context.
   */
  static java.util.List<Integer> findExpiredRequestIds(
      java.util.Map<Integer, UserRequest> requests, long deadlineMs, long nowMs) {
    if (requests.isEmpty() || deadlineMs <= 0) {
      return java.util.Collections.emptyList();
    }
    java.util.List<Integer> expired = new java.util.ArrayList<>();
    for (Entry<Integer, UserRequest> e : requests.entrySet()) {
      UserRequest req = e.getValue();
      if (req == null) continue;
      if (nowMs - req.getCreateTs() > deadlineMs) {
        expired.add(e.getKey());
      }
    }
    return expired;
  }

  /** @return total user requests failed by the deadline watchdog since adapter start. */
  public long getDeadlineWatchdogFailedCount() {
    return watchdogFailedCount.get();
  }

  /**
   * Stop the adapter and fail all outstanding futures.
   */
  public void stop() {
    if (stopped.getAndSet(true)) {
      return;
    }
    if (deadlineWatchdog != null) {
      deadlineWatchdog.cancel(false);
      deadlineWatchdog = null;
    }
    if (deadlineScheduler != null) {
      deadlineScheduler.shutdownNow();
      deadlineScheduler = null;
    }
    clear();
  }

  public void clear() {
    clear("Clear");
  }

  /**
   * Fail every outstanding request with {@code errorLog} and drop all bookkeeping for it.
   *
   * <p>Implemented as an iterator-based, per-entry remove rather than a wipe-then-clear so that
   * a concurrent submission that lands a {@code put(requestId, ...)} <em>while</em> this method
   * is running cannot be silently dropped from one of the three maps and then leak (with no
   * one to ever fail its future). The trade-off is that we only guarantee to fail requests
   * that were visible at the time their map entry was visited; a brand-new request can still
   * race in. Callers that need a hard barrier should additionally call {@link #stop()}.</p>
   */
  public void clear(String errorLog) {
    int failed = 0;
    for (Iterator<Entry<Integer, FutureResult>> it = requestIdToResultMap.entrySet().iterator();
        it.hasNext();) {
      Entry<Integer, FutureResult> e = it.next();
      int rid = e.getKey();
      FutureResult fr = e.getValue();
      // Drop bookkeeping first, then signal the future. This ordering means a concurrent
      // response that wins the CAS on FutureResult.set still finds the maps empty and the
      // duplicate-set is harmlessly ignored.
      it.remove();
      requestIdToResponseCache.remove(rid);
      requests.remove(rid);
      if (fr != null) {
        fr.setExecuteException(new ExecutionException(new AngelException(errorLog)));
        failed++;
      }
    }
    if (failed > 0) {
      LOG.warn("UserRequestAdapter.clear failed " + failed + " outstanding request(s): "
          + errorLog);
    }
  }


  private MatrixTransportInterface chooseTransportClient(int matrixId, int partId) {
    if (deployMode == AngelDeployMode.EMBEDDED) {
      PSContext psContext = EmbeddedParameterServer.getInstance().getPsContext();
      if(psContext == null) {
        // Use remote client instead
        return PSAgentContext.get().getMatrixTransportClient();
      }

      LocalTransportClient localPSClient = EmbeddedParameterServer.getInstance()
          .getLocalPSClient();

      // Check matrix id and partition id
      MatrixMeta matrixMeta = psContext.getMatrixMetaManager().getMatrixMeta(matrixId);
      if (matrixMeta != null) {
        PartitionKey[] parts = matrixMeta.getPartitionKeys();
        for (PartitionKey part : parts) {
          if (part.getPartitionId() == partId) {
            return localPSClient;
          }
        }
      }
    }

    return PSAgentContext.get().getMatrixTransportClient();
  }


  /**
   * Get elements of the row use int indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowId row id
   * @param indices elements indices
   * @return the Vector use sparse storage, contains indices and values
   */
  public FutureResult<Vector> get(int matrixId, int rowId, int[] indices) throws AngelException {
    checkParams(matrixId, rowId, indices);
    return get(new IntIndexGetRowRequest(matrixId, rowId, indices, null));
  }

  /**
   * Get elements of the row use int indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowId row id
   * @param indices elements indices
   * @param func element init function
   * @return the Vector use sparse storage, contains indices and values
   */
  public FutureResult<Vector> get(int matrixId, int rowId, int[] indices, InitFunc func)
      throws AngelException {
    checkParams(matrixId, rowId, indices);
    return get(new IntIndexGetRowRequest(matrixId, rowId, indices, func));
  }

  /**
   * Get elements of the row use long indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowId row id
   * @param indices elements indices
   * @return the Vector use sparse storage, contains indices and values
   */
  public FutureResult<Vector> get(int matrixId, int rowId, long[] indices) throws AngelException {
    checkParams(matrixId, rowId, indices);
    return get(new LongIndexGetRowRequest(matrixId, rowId, indices, null));
  }

  /**
   * Get elements of the row use long indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowId row id
   * @param indices elements indices
   * @param func element init function
   * @return the Vector use sparse storage, contains indices and values
   */
  public FutureResult<Vector> get(int matrixId, int rowId, long[] indices, InitFunc func)
      throws AngelException {
    checkParams(matrixId, rowId, indices);
    return get(new LongIndexGetRowRequest(matrixId, rowId, indices, func));
  }

  private FutureResult<Vector> get(IndexGetRowRequest request) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager()
        .getMatrixMeta(request.getMatrixId());

    PartitionKey[] parts = matrixMeta.getPartitionKeys();

    // Split the user request to partition requests
    FutureResult<Vector> result = new FutureResult<>();
    KeyPart[] splits;
    if (request instanceof IntIndexGetRowRequest) {
      splits = RouterUtils.split(matrixMeta, request.getRowId(),
          ((IntIndexGetRowRequest) request).getKeys());
    } else if (request instanceof LongIndexGetRowRequest) {
      splits = RouterUtils.split(matrixMeta, request.getRowId(),
          ((LongIndexGetRowRequest) request).getKeys());
    } else {
      throw new UnsupportedOperationException(
          "Unsupport index request type " + request.getClass().toString());
    }

    if (parts.length != splits.length) {
      throw new AngelException("partition count mismatch for matrix " + request.getMatrixId()
          + ": parts=" + parts.length + ", splits=" + splits.length);
    }

    // filter empty partition requests
    int needRequestPartNum = noEmptyPartNum(splits);
    if (needRequestPartNum == 0) {
      result.set(null);
      return result;
    }

    int requestId = registerRequest(request, result, needRequestPartNum);

    // Send all the partition requests
    for (int i = 0; i < splits.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        sendIndexGetRowRequest(requestId, request.getMatrixId(),
            request.getRowId(), parts[i].getPartitionId(), splits[i], request.getFunc());
      }
    }

    return result;
  }

  /**
   * Register a user-level request and its result/cache in the three bookkeeping maps using a
   * <em>fixed</em> order: {@code requests} → {@code responseCache} → {@code resultMap}. The
   * response side only reads {@code resultMap} as the gate, which guarantees that whenever a
   * response thread sees a non-null FutureResult the other two entries are already visible
   * to it.
   */
  private <T> int registerRequest(UserRequest request, FutureResult<T> result, int needPartNum) {
    if (stopped.get()) {
      AngelException ex = new AngelException("UserRequestAdapter is stopped");
      result.setExecuteException(new ExecutionException(ex));
      throw ex;
    }
    int requestId = request.getRequestId();
    requests.put(requestId, request);
    requestIdToResponseCache.put(requestId, new MapResponseCache(needPartNum));
    requestIdToResultMap.put(requestId, result);
    return requestId;
  }

  private RequestHeader createRequestHeader(int requestId, TransportMethod method, int matrixId,
      int partId) {
    RequestHeader header = new RequestHeader();
    header.setUserRequestId(requestId);
    header.setMethodId(method.getMethodId());
    header.setMatrixId(matrixId);
    header.setPartId(partId);
    return header;
  }

  /**
   * Build the {@link Request} envelope from {@code (method, matrixId, partId, body)} and hand it
   * to the chosen transport. Used by every {@code sendXxxRequest} below. Centralising the
   * envelope build removes ~25 lines of copy-paste per partition-method that previously had to
   * be kept in sync (header build, body wrap, client lookup, dispatch).
   */
  private void dispatch(int userRequestId, TransportMethod method, int matrixId, int partId,
      RequestData body, boolean isUpdate) {
    MatrixTransportInterface client = chooseTransportClient(matrixId, partId);
    Request request = new Request(createRequestHeader(userRequestId, method, matrixId, partId),
        body);
    if (isUpdate) {
      client.sendUpdateRequest(request);
    } else {
      client.sendGetRequest(request);
    }
  }

  private void sendIndexGetRowRequest(int userRequestId, int matrixId, int rowId, int partId,
      KeyPart keyPart, InitFunc func) {
    dispatch(userRequestId, TransportMethod.INDEX_GET_ROW, matrixId, partId,
        new IndexPartGetRowRequest(rowId, keyPart, func), false);
  }

  /**
   * Get elements of the rows use int indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowIds rows ids
   * @param indices elements indices
   * @return the Vectors use sparse storage, contains indices and values
   */
  public FutureResult<Vector[]> get(int matrixId, int[] rowIds, int[] indices)
      throws AngelException {
    checkParams(matrixId, rowIds, indices);
    return get(new IntIndexGetRowsRequest(matrixId, rowIds, indices, null));
  }

  /**
   * Get elements of the rows use int indices, the row type should has "int" type indices
   *
   * @param matrixId matrix id
   * @param rowIds rows ids
   * @param indices elements indices
   * @param func element init function
   * @return the Vectors use sparse storage, contains indices and values
   */
  public FutureResult<Vector[]> get(int matrixId, int[] rowIds, int[] indices, InitFunc func)
      throws AngelException {
    checkParams(matrixId, rowIds, indices);
    return get(new IntIndexGetRowsRequest(matrixId, rowIds, indices, func));
  }

  /**
   * Get elements of the rows use long indices, the row type should has "long" type indices
   *
   * @param matrixId matrix id
   * @param rowIds rows ids
   * @param indices elements indices
   * @return the Vectors use sparse storage, contains indices and values
   */
  public FutureResult<Vector[]> get(int matrixId, int[] rowIds, long[] indices)
      throws AngelException {
    checkParams(matrixId, rowIds, indices);
    return get(new LongIndexGetRowsRequest(matrixId, rowIds, indices, null));
  }

  /**
   * Get elements of the rows use long indices, the row type should has "long" type indices
   *
   * @param matrixId matrix id
   * @param rowIds rows ids
   * @param indices elements indices
   * @param func element init function
   * @return the Vectors use sparse storage, contains indices and values
   */
  public FutureResult<Vector[]> get(int matrixId, int[] rowIds, long[] indices, InitFunc func)
      throws AngelException {
    checkParams(matrixId, rowIds, indices);
    return get(new LongIndexGetRowsRequest(matrixId, rowIds, indices, func));
  }

  private FutureResult<Vector[]> get(IndexGetRowsRequest request) {
    // Only support column-partitioned matrix now!!
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager()
        .getMatrixMeta(request.getMatrixId());

    PartitionKey[] matrixParts = matrixMeta.getPartitionKeys();
    for (PartitionKey part : matrixParts) {
      if (part.getStartRow() != 0 || part.getEndRow() != matrixMeta.getRowNum()) {
        throw new UnsupportedOperationException(
            "Get rows by indices only support column-partitioned matrix now");
      }
    }

    // Split the user request to partition requests
    FutureResult<Vector[]> result = new FutureResult<>();
    KeyPart[] splits;
    if (request instanceof IntIndexGetRowsRequest) {
      splits = RouterUtils.split(matrixMeta, -1, ((IntIndexGetRowsRequest) request).getIndices());
    } else if (request instanceof LongIndexGetRowsRequest) {
      splits = RouterUtils.split(matrixMeta, -1, ((LongIndexGetRowsRequest) request).getIndices());
    } else {
      throw new UnsupportedOperationException(
          "Unsupport index request type " + request.getClass().toString());
    }

    if (matrixParts.length != splits.length) {
      throw new AngelException("partition count mismatch for matrix " + request.getMatrixId()
          + ": parts=" + matrixParts.length + ", splits=" + splits.length);
    }

    // filter empty partition requests
    int needRequestPartNum = noEmptyPartNum(splits);
    if (needRequestPartNum == 0) {
      result.set(null);
      return result;
    }

    int requestId = registerRequest(request, result, needRequestPartNum);

    for (int i = 0; i < splits.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        sendIndexGetRowsRequest(requestId, request.getMatrixId(), request.getRowIds(),
            matrixParts[i].getPartitionId(), splits[i], request.getFunc());
      }
    }
    return result;
  }

  private void sendIndexGetRowsRequest(int userRequestId, int matrixId, int[] rowIds, int partId,
      KeyPart keyPart, InitFunc func) {
    dispatch(userRequestId, TransportMethod.INDEX_GET_ROWS, matrixId, partId,
        new IndexPartGetRowsRequest(rowIds, keyPart, func), false);
  }


  /**
   * Get a row from ps use a udf.
   *
   * @param func get row udf
   * @return GetResult the result of the udf
   * @throws ExecutionException exception thrown when attempting to retrieve the result of a task
   * that aborted by throwing an exception
   * @throws InterruptedException interrupted while wait the result
   */
  public FutureResult<GetResult> get(GetFunc func) throws InterruptedException, ExecutionException {
    GetParam param = func.getParam();

    // Split param use matrix partitons
    List<PartitionGetParam> partParams = param.split();
    int size = partParams.size();
    if(size == 0) {
      throw new InvalidParameterException("Split number for " + param.getClass().getName()
          + " is zero, please check the parameters of function");
    }

    GetPSFRequest request = new GetPSFRequest(func);
    FutureResult<GetResult> result = new FutureResult<>();
    int requestId = registerRequest(request, result, size);

    for (int i = 0; i < size; i++) {
      sendGetUDFRequest(requestId, partParams.get(i).getMatrixId(),
          partParams.get(i).getPartKey().getPartitionId(), func, partParams.get(i));
    }
    return result;
  }

  private void sendGetUDFRequest(int userRequestId, int matrixId, int partId, GetFunc func,
      PartitionGetParam partGetParam) {
    dispatch(userRequestId, TransportMethod.GET_PSF, matrixId, partId,
        new GetUDFRequest(func.getClass().getName(), partGetParam, func.getParam()), false);
  }

  public FutureResult<Vector> getRow(int matrixId, int rowId) {
    checkParams(matrixId, rowId);

    // Get partitions for this row
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowId);
    GetRowRequest request = new GetRowRequest(matrixId, rowId);
    FutureResult<Vector> result = new FutureResult<>();
    int requestId = registerRequest(request, result, parts.size());

    for (PartitionKey part : parts) {
      sendGetRowRequest(requestId, part.getMatrixId(), rowId, part.getPartitionId());
    }

    return result;
  }

  private void sendGetRowRequest(int userRequestId, int matrixId, int rowId, int partId) {
    dispatch(userRequestId, TransportMethod.GET_ROWSPLIT, matrixId, partId,
        new GetRowSplitRequest(rowId), false);
  }

  public FutureResult<Vector[]> getRows(int matrixId, int[] rowIds) {
    checkParams(matrixId, rowIds);

    // The rows may be located in different row blocks, so every partition only gets the rows it
    // actually holds; the response side merges the row splits by row id.
    PartitionKey[] matrixParts = PSAgentContext.get().getMatrixMetaManager()
        .getMatrixMeta(matrixId).getPartitionKeys();
    List<PartitionKey> parts = new ArrayList<>();
    List<int[]> partRowIds = new ArrayList<>();
    for (PartitionKey part : matrixParts) {
      int num = 0;
      for (int rowId : rowIds) {
        if (rowId >= part.getStartRow() && rowId < part.getEndRow()) {
          num++;
        }
      }
      if (num == 0) {
        continue;
      }

      int[] subRowIds;
      if (num == rowIds.length) {
        subRowIds = rowIds;
      } else {
        subRowIds = new int[num];
        int index = 0;
        for (int rowId : rowIds) {
          if (rowId >= part.getStartRow() && rowId < part.getEndRow()) {
            subRowIds[index++] = rowId;
          }
        }
      }
      parts.add(part);
      partRowIds.add(subRowIds);
    }

    GetRowsRequest request = new GetRowsRequest(matrixId, rowIds);
    FutureResult<Vector[]> result = new FutureResult<>();
    int requestId = registerRequest(request, result, parts.size());

    for (int i = 0; i < parts.size(); i++) {
      sendGetRowsRequest(requestId, parts.get(i).getMatrixId(), partRowIds.get(i),
          parts.get(i).getPartitionId());
    }

    return result;
  }

  private void sendGetRowsRequest(int userRequestId, int matrixId, int[] rowIds, int partId) {
    dispatch(userRequestId, TransportMethod.GET_ROWSSPLIT, matrixId, partId,
        new GetRowsSplitRequest(rowIds), false);
  }

  /**
   * Update matrix use a udf.
   *
   * @param updateFunc update udf function
   * @return Future<VoidResult> update future result
   */
  public Future<VoidResult> update(UpdateFunc updateFunc) {
    UpdateParam param = updateFunc.getParam();

    // Split the param use matrix partitions
    List<PartitionUpdateParam> partParams = param.split();
    int size = partParams.size();
    if(size == 0) {
      throw new InvalidParameterException("Split number for " + param.getClass().getName()
          + " is zero, please check the parameters of function");
    }

    UpdatePSFRequest request = new UpdatePSFRequest(updateFunc);
    FutureResult<VoidResult> result = new FutureResult<>();
    int requestId = registerRequest(request, result, size);

    // Send request to PSS
    for (int i = 0; i < size; i++) {
      sendUpdateUDFRequest(requestId, partParams.get(i).getMatrixId(),
          partParams.get(i).getPartKey().getPartitionId(), updateFunc, partParams.get(i));
    }

    return result;
  }

  private void sendUpdateUDFRequest(int userRequestId, int matrixId, int partId,
      UpdateFunc updateFunc, PartitionUpdateParam partParam) {
    dispatch(userRequestId, TransportMethod.UPDATE_PSF, matrixId, partId,
        new UpdateUDFRequest(updateFunc.getClass().getName(), partParam), true);
  }

  public Future<VoidResult> update(int matrixId, int rowId, Vector delta, UpdateOp op) {
    checkParams(matrixId, rowId);
    delta.setMatrixId(matrixId);
    delta.setRowId(rowId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = matrixMeta.getPartitionKeys();
    CompStreamKeyValuePart[] splits = RouterUtils.splitStream(matrixMeta, delta);
    if (parts.length != splits.length) {
      throw new AngelException("partition count mismatch for matrix " + matrixId
          + ": parts=" + parts.length + ", splits=" + splits.length);
    }

    FutureResult<VoidResult> result = new FutureResult<>();
    int needRequestPartNum = noEmptyPartNum(splits);
    if (needRequestPartNum == 0) {
      result.set(null);
      return result;
    }

    UpdateRowRequest request = new UpdateRowRequest(matrixId, rowId, op);
    int requestId = registerRequest(request, result, needRequestPartNum);

    for (int i = 0; i < splits.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        sendUpdateRequest(requestId, matrixId, parts[i].getPartitionId(), splits[i], op);
      }
    }
    return result;
  }

  private void sendUpdateRequest(int userRequestId, int matrixId, int partId,
      CompStreamKeyValuePart split, UpdateOp op) {
    dispatch(userRequestId, TransportMethod.UPDATE, matrixId, partId,
        new UpdateRequest(split, op), true);
  }

  public Future<VoidResult> update(int matrixId, Matrix delta, UpdateOp op) {
    checkParams(matrixId);

    delta.setMatrixId(matrixId);
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = matrixMeta.getPartitionKeys();
    CompStreamKeyValuePart[] splits = RouterUtils.splitStream(matrixMeta, delta);
    if (parts.length != splits.length) {
      throw new AngelException("partition count mismatch for matrix " + matrixId
          + ": parts=" + parts.length + ", splits=" + splits.length);
    }

    int needRequestPartNum = noEmptyPartNum(splits);
    FutureResult<VoidResult> result = new FutureResult<>();
    if (needRequestPartNum == 0) {
      result.set(null);
      return result;
    }

    UpdateMatrixRequest request = new UpdateMatrixRequest(matrixId, op);
    int requestId = registerRequest(request, result, needRequestPartNum);

    for (int i = 0; i < splits.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        sendUpdateRequest(requestId, matrixId, parts[i].getPartitionId(), splits[i], op);
      }
    }
    return result;
  }

  public Future<VoidResult> update(int matrixId, int[] rowIds, Vector[] rows, UpdateOp op) {
    assert rowIds.length == rows.length;
    checkParams(matrixId, rowIds);

    for (int i = 0; i < rowIds.length; i++) {
      rows[i].setRowId(rowIds[i]);
    }

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = matrixMeta.getPartitionKeys();
    CompStreamKeyValuePart[] splits = RouterUtils.splitStream(matrixMeta, rows);
    if (parts.length != splits.length) {
      throw new AngelException("partition count mismatch for matrix " + matrixId
          + ": parts=" + parts.length + ", splits=" + splits.length);
    }

    int needRequestPartNum = noEmptyPartNum(splits);
    FutureResult<VoidResult> result = new FutureResult<>();
    if (needRequestPartNum == 0) {
      result.set(null);
      return result;
    }

    UpdateRowsRequest request = new UpdateRowsRequest(matrixId, op);
    int requestId = registerRequest(request, result, needRequestPartNum);

    for (int i = 0; i < splits.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        sendUpdateRequest(requestId, matrixId, parts[i].getPartitionId(), splits[i], op);
      }
    }
    return result;
  }

  private void checkParams(int matrixId) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    if (matrixMeta == null) {
      throw new AngelException("can not find matrix " + matrixId);
    }
  }

  private void checkParams(int matrixId, int rowId) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    if (matrixMeta == null) {
      throw new AngelException("can not find matrix " + matrixId);
    }
    int rowNum = matrixMeta.getRowNum();
    if (rowId < 0 || rowId >= rowNum) {
      throw new AngelException("not valid row id, row id is in range[0," + rowNum + ")");
    }
  }

  private void checkParams(int matrixId, int[] rowIds) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    if (matrixMeta == null) {
      throw new AngelException("can not find matrix " + matrixId);
    }

    if (rowIds == null || rowIds.length == 0) {
      throw new AngelException("row ids is empty");
    }

    int rowNum = matrixMeta.getRowNum();
    for (int rowId : rowIds) {
      if (rowId < 0 || rowId >= rowNum) {
        throw new AngelException("not valid row id, row id is in range[0," + rowNum + ")");
      }
    }
  }

  private void checkParams(int matrixId, int rowId, int[] colIds) {
    checkParams(matrixId, rowId);
    if (colIds == null || colIds.length == 0) {
      throw new AngelException("column indice can not be null");
    }
  }

  private void checkParams(int matrixId, int rowId, long[] colIds) {
    checkParams(matrixId, rowId);
    if (colIds == null || colIds.length == 0) {
      throw new AngelException("column indice can not be null");
    }
  }

  private void checkParams(int matrixId, int[] rowIds, int[] colIds) {
    checkParams(matrixId, rowIds);
    if (colIds == null || colIds.length == 0) {
      throw new AngelException("column indice can not be null");
    }
  }

  private void checkParams(int matrixId, int[] rowIds, long[] colIds) {
    checkParams(matrixId, rowIds);
    if (colIds == null || colIds.length == 0) {
      throw new AngelException("column indice can not be null");
    }
  }

  /**
   * Notify the caller of a permanent failure and drop all bookkeeping for {@code requestId}.
   *
   * <p>Drops the maps BEFORE signalling the future so that any concurrent in-flight handler
   * that observes a null entry knows to release its own per-seqId resources rather than
   * leaking them (see the FATAL branch in {@code MatrixTransportClient.Responser}).</p>
   */
  public void requestError(int requestId, String errorLog) {
    FutureResult result = requestIdToResultMap.remove(requestId);
    requestIdToResponseCache.remove(requestId);
    UserRequest req = requests.remove(requestId);
    if (result != null) {
      result.setExecuteException(new ExecutionException(errorLog, new AngelException(errorLog)));
      // INFO -- this is the user-visible failure point. Including the request signature
      // (id + type) lets users correlate "future failed" with which partitioned request died
      // without having to enable DEBUG on the full transport stack.
      LOG.info("user request " + requestId
          + (req != null ? " (type=" + req.getType() + ")" : "")
          + " failed: " + errorLog);
    } else if (LOG.isDebugEnabled()) {
      // result == null means the entry was already evicted by a prior failure / clear; the
      // future has already been completed by whoever did the eviction, so nothing to do --
      // but log at DEBUG so the duplicate-fail can be correlated when chasing leaks.
      LOG.debug("requestError for already-cleared requestId " + requestId + ": " + errorLog);
    }
  }

  public void clear(int requestId) {
    requests.remove(requestId);
    requestIdToResponseCache.remove(requestId);
    requestIdToResultMap.remove(requestId);
  }

  public UserRequest getUserRequest(int userRequestId) {
    return requests.get(userRequestId);
  }

  public ResponseCache getResponseCache(int userRequestId) {
    return requestIdToResponseCache.get(userRequestId);
  }

  public FutureResult getFutureResult(int userRequestId) {
    return requestIdToResultMap.get(userRequestId);
  }

  private int noEmptyPartNum(DataPart[] dataParts) {
    if (dataParts == null) {
      return 0;
    }

    int count = 0;
    for (int i = 0; i < dataParts.length; i++) {
      if (dataParts[i] != null && dataParts[i].size() > 0) {
        count++;
      }
    }
    return count;
  }
}
