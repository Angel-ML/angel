package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.ServerState;
import com.tencent.angel.ps.server.data.TransportMethod;
import com.tencent.angel.ps.server.data.handler.BasicStreamUpdateHandler;
import com.tencent.angel.ps.server.data.handler.GetPartHandler;
import com.tencent.angel.ps.server.data.handler.GetRowHandler;
import com.tencent.angel.ps.server.data.handler.GetRowsHandler;
import com.tencent.angel.ps.server.data.handler.Handler;
import com.tencent.angel.ps.server.data.handler.IndexGetRowHandler;
import com.tencent.angel.ps.server.data.handler.IndexGetRowsHandler;
import com.tencent.angel.ps.server.data.handler.PSFGetHandler;
import com.tencent.angel.ps.server.data.handler.PSFUpdateHandler;
import com.tencent.angel.ps.server.data.profiling.ServerTimes;
import com.tencent.angel.ps.server.data.request.GetUDFRequest;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.ResponseHeader;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequest;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequestAdapter;
import com.tencent.angel.psagent.matrix.transport.response.GetUDFHandler;
import com.tencent.angel.psagent.matrix.transport.response.ResponseCache;
import com.tencent.angel.psagent.matrix.transport.response.UpdateHandler;
import com.tencent.angel.psagent.matrix.transport.response.UpdateUDFHandler;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class LocalTransportClient implements MatrixTransportInterface {

  private final PSContext psContext;

  private final Map<Integer, Handler> methodIdToRequestHandlers;
  private final Map<Integer, com.tencent.angel.psagent.matrix.transport.response.Handler> methodIdToResponseHanders;

  public LocalTransportClient(PSContext psContext) {
    this.psContext = psContext;
    this.methodIdToRequestHandlers = new HashMap<>();
    registerRequestHanders();
    this.methodIdToResponseHanders = new HashMap<>();
    registeResponseHandler();
  }

  private void registerRequestHanders() {
    methodIdToRequestHandlers
        .put(TransportMethod.GET_ROWSPLIT.getMethodId(), new GetRowHandler(psContext));
    methodIdToRequestHandlers
        .put(TransportMethod.GET_ROWSSPLIT.getMethodId(), new GetRowsHandler(psContext));
    methodIdToRequestHandlers
        .put(TransportMethod.INDEX_GET_ROW.getMethodId(), new IndexGetRowHandler(psContext));
    methodIdToRequestHandlers
        .put(TransportMethod.INDEX_GET_ROWS.getMethodId(), new IndexGetRowsHandler(psContext));
    methodIdToRequestHandlers
        .put(TransportMethod.GET_PSF.getMethodId(), new PSFGetHandler(psContext));
    methodIdToRequestHandlers
        .put(TransportMethod.UPDATE_PSF.getMethodId(), new PSFUpdateHandler(psContext));
    methodIdToRequestHandlers
        .put(TransportMethod.GET_PART.getMethodId(), new GetPartHandler(psContext));
    methodIdToRequestHandlers
        .put(TransportMethod.UPDATE.getMethodId(), new BasicStreamUpdateHandler(psContext));
  }

  private void registeResponseHandler() {
    PSAgentContext context = PSAgentContext.get();
    methodIdToResponseHanders.put(TransportMethod.GET_ROWSPLIT.getMethodId(),
        new com.tencent.angel.psagent.matrix.transport.response.GetRowHandler(context));
    methodIdToResponseHanders.put(TransportMethod.GET_ROWSSPLIT.getMethodId(),
        new com.tencent.angel.psagent.matrix.transport.response.GetRowsHandler(context));
    methodIdToResponseHanders.put(TransportMethod.INDEX_GET_ROW.getMethodId(),
        new com.tencent.angel.psagent.matrix.transport.response.IndexGetRowHandler(context));
    methodIdToResponseHanders.put(TransportMethod.INDEX_GET_ROWS.getMethodId(),
        new com.tencent.angel.psagent.matrix.transport.response.IndexGetRowsHandler(context));
    methodIdToResponseHanders.put(TransportMethod.UPDATE.getMethodId(), new UpdateHandler(context));
    methodIdToResponseHanders
        .put(TransportMethod.GET_PSF.getMethodId(), new GetUDFHandler(context));
    methodIdToResponseHanders
        .put(TransportMethod.UPDATE_PSF.getMethodId(), new UpdateUDFHandler(context));
  }


  @Override
  public void sendGetRequest(Request request) {
    ExecutorService pool = psContext.getWorkerPool().getWorkerPool();
    if (pool == null) {
      // Run use current thread
      handle(request);
    } else {
      // Run use worker in pool
      pool.execute(new LocalRunner(request));
    }
    //psContext.getWorkerPool().getWorker().execute(new LocalRunner(request));
  }

  @Override
  public void sendUpdateRequest(Request request) {
    ExecutorService pool = psContext.getWorkerPool().getWorkerPool();
    if (pool == null) {
      // Run use current thread
      handle(request);
    } else {
      // Run use worker in pool
      pool.execute(new LocalRunner(request));
    }
    //psContext.getWorkerPool().getWorkerPool().execute(new LocalRunner(request));
  }

  class LocalRunner extends Thread {
    private final Request request;

    public LocalRunner(Request request) {
      this.request = request;
    }

    @Override
    public void run() {
      handle(request);
    }
  }

  public void handle(Request r) {
    Request cloneRequest = cloneRequest(r);

    int userRequestId = cloneRequest.getUserRequestId();
    UserRequestAdapter adapter = PSAgentContext.get().getUserRequestAdapter();
    UserRequest userRequest = adapter.getUserRequest(userRequestId);
    ResponseCache responseCache = adapter.getResponseCache(userRequestId);
    FutureResult futureResult = adapter.getFutureResult(userRequestId);

    if (userRequest == null || responseCache == null || futureResult == null) {
      // Some error happens, just return
      return;
    }

    int methodId = cloneRequest.getHeader().methodId;
    Handler requestHandler = methodIdToRequestHandlers.get(methodId);
    com.tencent.angel.psagent.matrix.transport.response.Handler responseHandler = methodIdToResponseHanders
        .get(methodId);
    try {
      // Handle
      ResponseData resultData = requestHandler
          .handle(cloneRequest.getHeader(), cloneRequest.getData());

      ResponseHeader header = new ResponseHeader(cloneRequest.getSeqId(), methodId, ServerState.GENERAL,
          com.tencent.angel.ps.server.data.response.ResponseType.SUCCESS);

      Response result = new Response(header, resultData, new ServerTimes());

      // Clone
      Response clonedRet = cloneResponse(result, r);

      // Add the response to the cache
      responseCache.add(cloneRequest, clonedRet);

      // Check can merge or not
      synchronized (responseCache) {
        if (responseCache.canMerge()) {
          // Merge
          responseHandler.handle(futureResult, userRequest, responseCache);

          // Clear response cache
          responseCache.clear();

          // Remove the response cache
          adapter.clear(cloneRequest.getUserRequestId());
        }
      }

    } catch (Throwable e) {
      futureResult.setExecuteException(new ExecutionException(e));

      // Clear response cache
      responseCache.clear();

      // Remove the response cache
      adapter.clear(cloneRequest.getUserRequestId());
    }
  }

  private Request cloneRequest(Request request) {
    byte[] serializedRequest = new byte[ByteBufSerdeUtils.serializedObjectLen(request)];
    ByteBuf buf = Unpooled.wrappedBuffer(serializedRequest);
    buf.resetWriterIndex();
    ByteBufSerdeUtils.serializeObject(buf, request);
    Request clonedRet = (Request) ByteBufSerdeUtils.deserializeObject(buf);
    buf.release();
    return clonedRet;
  }

  private Response cloneResponse(Response response, Request request) {
    byte[] serializedResponse = new byte[ByteBufSerdeUtils.serializedObjectLen(response)];
    ByteBuf buf = Unpooled.wrappedBuffer(serializedResponse);
    buf.resetWriterIndex();
    ByteBufSerdeUtils.serializeObject(buf, response);

    //Response clonedRet;
    //if (request.getData() instanceof GetUDFRequest) {
    //  clonedRet = (Response) ByteBufSerdeUtils.deserializeObjectForPartPSFResult(buf, (GetUDFRequest)request.getData());
    //} else {
    //  clonedRet = (Response) ByteBufSerdeUtils.deserializeObject(buf);
    //}
    Response clonedRet = ByteBufSerdeUtils.deserializeResponse(buf, request.getData());

    buf.release();
    return clonedRet;
  }

  @Override
  public FutureResult<PartitionGetResult> get(GetFunc func, PartitionGetParam param) {
    func.setPsContext(psContext);
    FutureResult<PartitionGetResult> resultFuture = new FutureResult<>();

    try {
      PartitionGetResult partResult = func.partitionGet(param);

      // Deep clone
      byte[] serializedResult = new byte[partResult.bufferLen()];
      ByteBuf buf = Unpooled.wrappedBuffer(serializedResult);
      ByteBufSerdeUtils.serializeObject(buf, partResult);
      PartitionGetResult clonedRet = (PartitionGetResult) ByteBufSerdeUtils.deserializeObject(buf);
      buf.release();

      resultFuture.set(clonedRet);
    } catch (Throwable e) {
      resultFuture.setExecuteException(new ExecutionException(e));
    }
    return resultFuture;
  }

  @Override
  public FutureResult<VoidResult> update(UpdateFunc func, PartitionUpdateParam param) {
    func.setPsContext(psContext);
    FutureResult<VoidResult> resultFuture = new FutureResult<>();

    try {
      func.partitionUpdate(param);
      resultFuture.set(new VoidResult(ResponseType.SUCCESS));
    } catch (Throwable e) {
      resultFuture.setExecuteException(new ExecutionException(e));
    }
    return resultFuture;
  }
}
