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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
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
import com.tencent.angel.ps.server.data.TransportMethod;
import com.tencent.angel.ps.server.data.request.GetRowSplitRequest;
import com.tencent.angel.ps.server.data.request.GetRowsSplitRequest;
import com.tencent.angel.ps.server.data.request.GetUDFRequest;
import com.tencent.angel.ps.server.data.request.IndexPartGetRowRequest;
import com.tencent.angel.ps.server.data.request.IndexPartGetRowsRequest;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.server.data.request.UpdateRequest;
import com.tencent.angel.ps.server.data.request.UpdateUDFRequest;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportClient;
import com.tencent.angel.psagent.matrix.transport.response.MapResponseCache;
import com.tencent.angel.psagent.matrix.transport.response.ResponseCache;
import com.tencent.angel.psagent.matrix.transport.router.CompStreamKeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.DataPart;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
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
   * matrix id to the lock for GET_ROWS request map
   */
  private final ConcurrentHashMap<Integer, ReentrantLock> locks;

  /**
   * Request id to request map
   */
  private final ConcurrentHashMap<Integer, UserRequest> requests;

  /**
   * Request id to result map
   */
  private final ConcurrentHashMap<Integer, FutureResult> requestIdToResultMap;

  /**
   * Response cache
   */
  private final ConcurrentHashMap<Integer, ResponseCache> requestIdToResponseCache;

  /**
   * Sub response merge worker pool
   */
  private volatile ForkJoinPool workerPool;


  /**
   * stop the merge dispatcher and all workers
   */
  private final AtomicBoolean stopped;

  /**
   * Create a new UserRequestAdapter.
   */
  public UserRequestAdapter() {
    locks = new ConcurrentHashMap<>();

    requests = new ConcurrentHashMap<>();
    requestIdToResultMap = new ConcurrentHashMap<>();
    requestIdToResponseCache = new ConcurrentHashMap<>();

    stopped = new AtomicBoolean(false);
  }

  /**
   * Start the sub-request results merge dispatcher.
   */
  public void start() {
    workerPool = new ForkJoinPool(16);
  }

  /**
   * Stop the merge dispatcher and all workers.
   */
  public void stop() {
    if (stopped.getAndSet(true)) {
      return;
    }

    if (workerPool != null) {
      workerPool.shutdownNow();
      workerPool = null;
    }

    clear();
  }

  public void clear() {
    clear("Clear");
  }

  public void clear(String errorLog) {
    for (Entry<Integer, FutureResult> resultEntry : requestIdToResultMap.entrySet()) {
      resultEntry.getValue()
          .setExecuteException(new ExecutionException(new AngelException(errorLog)));
    }

    requestIdToResultMap.clear();
    requestIdToResponseCache.clear();
    requests.clear();
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

    // Split the user request to partition requests
    FutureResult<Vector> result = new FutureResult<>();
    KeyPart[] splits;
    long startTs = System.currentTimeMillis();
    if (request instanceof IntIndexGetRowRequest) {
      splits = RouterUtils.split(matrixMeta, -1, ((IntIndexGetRowRequest) request).getKeys());
    } else if (request instanceof LongIndexGetRowRequest) {
      splits = RouterUtils.split(matrixMeta, -1, ((LongIndexGetRowRequest) request).getKeys());
    } else {
      throw new UnsupportedOperationException(
          "Unsupport index request type " + request.getClass().toString());
    }
    LOG.info("Get by indices split use time: " + (System.currentTimeMillis() - startTs));

    // filter empty partition requests
    int needRequestPartNum = noEmptyPartNum(splits);
    if (needRequestPartNum == 0) {
      result.set(null);
      return result;
    }

    // Create partition results cache
    ResponseCache cache = new MapResponseCache(needRequestPartNum);
    int requestId = request.getRequestId();
    requestIdToResponseCache.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);
    requests.put(requestId, request);

    // Send all the partition requests
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    for (int i = 0; i < splits.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        sendIndexGetRowRequest(matrixClient, requestId, request.getMatrixId(),
            request.getRowId(), i, splits[i], request.getFunc());
      }
    }

    return result;
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

  private void sendIndexGetRowRequest(MatrixTransportClient matrixClient, int userRequestId,
      int matrixId,
      int rowId, int partId, KeyPart keyPart, InitFunc func) {
    // Request header
    RequestHeader header = createRequestHeader(userRequestId, TransportMethod.INDEX_GET_ROW,
        matrixId, partId);

    // Request body
    IndexPartGetRowRequest requestData = new IndexPartGetRowRequest(rowId, keyPart, func);

    // Request
    Request request = new Request(header, requestData);

    // Send the request
    matrixClient.sendGetRequest(request);
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
    long startTs = System.currentTimeMillis();
    KeyPart[] splits;
    if (request instanceof IntIndexGetRowsRequest) {
      splits = RouterUtils.split(matrixMeta, -1, ((IntIndexGetRowsRequest) request).getIndices());
    } else if (request instanceof LongIndexGetRowsRequest) {
      splits = RouterUtils.split(matrixMeta, -1, ((LongIndexGetRowsRequest) request).getIndices());
    } else {
      throw new UnsupportedOperationException(
          "Unsupport index request type " + request.getClass().toString());
    }
    LOG.info("Get by indices split use time: " + (System.currentTimeMillis() - startTs));

    // filter empty partition requests
    int needRequestPartNum = noEmptyPartNum(splits);
    if (needRequestPartNum == 0) {
      result.set(null);
      return result;
    }

    // Create partition results cache
    ResponseCache cache = new MapResponseCache(needRequestPartNum);
    int requestId = request.getRequestId();
    requestIdToResponseCache.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);
    requests.put(requestId, request);

    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    for (int i = 0; i < splits.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        sendIndexGetRowsRequest(matrixClient, requestId, request.getMatrixId(), request.getRowIds(),
            i, splits[i], request.getFunc());
      }
    }
    return result;
  }

  private void sendIndexGetRowsRequest(MatrixTransportClient matrixClient, int userRequestId,
      int matrixId,
      int[] rowIds, int partId, KeyPart keyPart, InitFunc func) {
    // Request header
    RequestHeader header = createRequestHeader(userRequestId, TransportMethod.INDEX_GET_ROWS,
        matrixId, partId);

    // Request body
    IndexPartGetRowsRequest requestData = new IndexPartGetRowsRequest(rowIds, keyPart, func);

    // Request
    Request request = new Request(header, requestData);

    // Send the request
    matrixClient.sendGetRequest(request);
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
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    GetParam param = func.getParam();

    // Split param use matrix partitons
    List<PartitionGetParam> partParams = param.split();
    int size = partParams.size();

    GetPSFRequest request = new GetPSFRequest(func);

    int requestId = request.getRequestId();
    FutureResult<GetResult> result = new FutureResult<>();
    ResponseCache cache = new MapResponseCache(size);
    requests.put(requestId, request);
    requestIdToResponseCache.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);

    for (int i = 0; i < size; i++) {
      sendGetUDFRequest(matrixClient, requestId, partParams.get(i).getMatrixId(),
          partParams.get(i).getPartKey().getPartitionId(), func, partParams.get(i));
    }
    return result;
  }

  private void sendGetUDFRequest(MatrixTransportClient matrixClient, int userRequestId,
      int matrixId,
      int partId, GetFunc func, PartitionGetParam partGetParam) {
    // Request header
    RequestHeader header = createRequestHeader(userRequestId, TransportMethod.GET_PSF,
        matrixId, partId);

    // Request body
    GetUDFRequest requestData = new GetUDFRequest(func.getClass().getName(), partGetParam);

    // Request
    Request request = new Request(header, requestData);

    // Send the request
    matrixClient.sendGetRequest(request);
  }

  public FutureResult<Vector> getRow(int matrixId, int rowId) {
    checkParams(matrixId, rowId);

    // Get partitions for this row
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowId);
    GetRowRequest request = new GetRowRequest(matrixId, rowId);

    int requestId = request.getRequestId();
    FutureResult<Vector> result = new FutureResult<>();
    ResponseCache responseCache = new MapResponseCache(parts.size());
    requests.put(requestId, request);
    requestIdToResultMap.put(requestId, result);
    requestIdToResponseCache.put(requestId, responseCache);

    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    for (PartitionKey part : parts) {
      LOG.info("Get row " + part);
      sendGetRowRequest(matrixClient, requestId, part.getMatrixId(), rowId, part.getPartitionId());
    }

    return result;
  }

  private void sendGetRowRequest(MatrixTransportClient matrixClient, int userRequestId,
      int matrixId,
      int rowId, int partId) {
    // Request header
    RequestHeader header = createRequestHeader(userRequestId, TransportMethod.GET_ROWSPLIT,
        matrixId, partId);

    // Request body
    GetRowSplitRequest requestData = new GetRowSplitRequest(rowId);

    // Request
    Request request = new Request(header, requestData);

    // Send the request
    matrixClient.sendGetRequest(request);
  }

  public FutureResult<Vector[]> getRows(int matrixId, int[] rowIds) {
    checkParams(matrixId, rowIds);

    // Get partitions for this row
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowIds[0]);
    GetRowsRequest request = new GetRowsRequest(matrixId, rowIds);

    int requestId = request.getRequestId();
    FutureResult<Vector[]> result = new FutureResult<>();
    ResponseCache responseCache = new MapResponseCache(parts.size());
    requests.put(requestId, request);
    requestIdToResultMap.put(requestId, result);
    requestIdToResponseCache.put(requestId, responseCache);

    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    for (PartitionKey part : parts) {
      LOG.info("Get row " + part);
      sendGetRowsRequest(matrixClient, requestId, part.getMatrixId(), rowIds,
          part.getPartitionId());
    }

    return result;
  }

  private void sendGetRowsRequest(MatrixTransportClient matrixClient, int userRequestId,
      int matrixId,
      int[] rowIds, int partId) {
    // Request header
    RequestHeader header = createRequestHeader(userRequestId, TransportMethod.GET_ROWSSPLIT,
        matrixId, partId);

    // Request body
    GetRowsSplitRequest requestData = new GetRowsSplitRequest(rowIds);

    // Request
    Request request = new Request(header, requestData);

    // Send the request
    matrixClient.sendGetRequest(request);
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

    // Split the param use matrix partitions
    List<PartitionUpdateParam> partParams = param.split();

    int size = partParams.size();
    UpdatePSFRequest request = new UpdatePSFRequest(updateFunc);
    ResponseCache cache = new MapResponseCache(size);
    FutureResult<VoidResult> result = new FutureResult<>();
    int requestId = request.getRequestId();

    requests.put(requestId, request);
    requestIdToResponseCache.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);

    // Send request to PSS
    for (int i = 0; i < size; i++) {
      sendUpdateUDFRequest(matrixClient, requestId, partParams.get(i).getMatrixId(),
          partParams.get(i).getPartKey().getPartitionId(), updateFunc, partParams.get(i));
    }

    return result;
  }

  private void sendUpdateUDFRequest(MatrixTransportClient matrixClient, int userRequestId,
      int matrixId, int partId, UpdateFunc updateFunc, PartitionUpdateParam partParam) {
    // Request header
    RequestHeader header = createRequestHeader(userRequestId, TransportMethod.UPDATE_PSF,
        matrixId, partId);

    // Request body
    UpdateUDFRequest requestData = new UpdateUDFRequest(updateFunc.getClass().getName(), partParam);

    // Request
    Request request = new Request(header, requestData);

    // Send the request
    matrixClient.sendUpdateRequest(request);
  }

  public Future<VoidResult> update(int matrixId, int rowId, Vector delta, UpdateOp op) {
    checkParams(matrixId, rowId);
    delta.setMatrixId(matrixId);
    delta.setRowId(rowId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    CompStreamKeyValuePart[] splits = RouterUtils.splitStream(matrixMeta, delta);

    FutureResult<VoidResult> result = new FutureResult<>();
    int needRequestPartNum = noEmptyPartNum(splits);
    if (needRequestPartNum == 0) {
      result.set(null);
      return result;
    }

    UpdateRowRequest request = new UpdateRowRequest(matrixId, rowId, op);
    ResponseCache cache = new MapResponseCache(needRequestPartNum);
    int requestId = request.getRequestId();
    requestIdToResponseCache.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);
    requests.put(requestId, request);

    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    for (int i = 0; i < splits.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        sendUpdateRequest(matrixClient, requestId, matrixId, i, splits[i], op);
      }
    }
    return result;
  }

  private void sendUpdateRequest(MatrixTransportClient matrixClient, int userRequestId,
      int matrixId,
      int partId, CompStreamKeyValuePart split, UpdateOp op) {
    // Request header
    RequestHeader header = createRequestHeader(userRequestId, TransportMethod.UPDATE,
        matrixId, partId);

    // Request body
    UpdateRequest requestData = new UpdateRequest(split, op);

    // Request
    Request request = new Request(header, requestData);

    // Send the request
    matrixClient.sendUpdateRequest(request);
  }

  public Future<VoidResult> update(int matrixId, Matrix delta, UpdateOp op) {
    checkParams(matrixId);

    delta.setMatrixId(matrixId);
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    CompStreamKeyValuePart[] splits = RouterUtils.splitStream(matrixMeta, delta);

    int needRequestPartNum = noEmptyPartNum(splits);
    FutureResult<VoidResult> result = new FutureResult<>();
    if (needRequestPartNum == 0) {
      result.set(null);
      return result;
    }

    UpdateMatrixRequest request = new UpdateMatrixRequest(matrixId, op);
    ResponseCache cache = new MapResponseCache(needRequestPartNum);
    int requestId = request.getRequestId();
    requestIdToResponseCache.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);
    requests.put(requestId, request);

    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    for (int i = 0; i < splits.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        sendUpdateRequest(matrixClient, requestId, matrixId, i, splits[i], op);
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
    CompStreamKeyValuePart[] splits = RouterUtils.splitStream(matrixMeta, rows);

    int needRequestPartNum = noEmptyPartNum(splits);
    FutureResult<VoidResult> result = new FutureResult<>();
    if (needRequestPartNum == 0) {
      result.set(null);
      return result;
    }

    UpdateRowsRequest request = new UpdateRowsRequest(matrixId, op);
    ResponseCache cache = new MapResponseCache(needRequestPartNum);
    int requestId = request.getRequestId();
    requestIdToResponseCache.put(requestId, cache);
    requestIdToResultMap.put(requestId, result);
    requests.put(requestId, request);

    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    for (int i = 0; i < splits.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        sendUpdateRequest(matrixClient, requestId, matrixId, i, splits[i], op);
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

  private ReentrantLock getLock(int matrixId) {
    if (!locks.containsKey(matrixId)) {
      locks.putIfAbsent(matrixId, new ReentrantLock());
    }
    return locks.get(matrixId);
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
