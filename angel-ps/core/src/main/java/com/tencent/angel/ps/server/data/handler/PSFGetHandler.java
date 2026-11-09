package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.metrics.AngelPSLoad;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.WorkerPool;
import com.tencent.angel.ps.server.data.profiling.MatrixTransportServerMetrics;
import com.tencent.angel.ps.server.data.request.GetUDFRequest;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.response.GetUDFResponse;
import com.tencent.angel.ps.server.data.response.ResponseData;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PSFGetHandler extends Handler {

  private static final Log LOG = LogFactory.getLog(WorkerPool.class);
  private static final ConcurrentHashMap<String, Constructor<? extends GetFunc>> GET_FUNC_CTOR_CACHE =
      new ConcurrentHashMap<>();
  private final boolean metricEnable;
  /**
   * Process-wide PS RPC metrics singleton. Cached as a final reference so the hot
   * path doesn't repeat the static getInstance() call. The recordPsf method is a
   * cheap no-op when the operator hasn't enabled per-PSF breakdown.
   */
  private final MatrixTransportServerMetrics rpcMetrics =
      MatrixTransportServerMetrics.getInstance();

  public PSFGetHandler(PSContext context) {
    super(context);
    this.metricEnable = context.getMetricsEnable();
  }

  @Override
  public RequestData parseRequest(ByteBuf in) {
    GetUDFRequest request = new GetUDFRequest();
    request.deserialize(in);
    return request;
  }

  @Override
  public ResponseData handle(RequestHeader header, RequestData data) throws Exception {
    GetUDFRequest request = (GetUDFRequest) data;
    Constructor<? extends GetFunc> constructor = resolveConstructor(request.getGetFuncClass());
    GetFunc func = constructor.newInstance();
    Class<? extends GetFunc> funcClass = constructor.getDeclaringClass();
    //LOG.info("Get PSF func = " + func.getClass().getName());
    func.setPsContext(context);

    // Per-PSF-class metrics: time only the partitionGet body (the dominant cost).
    // Use funcClass.getName() rather than request.getGetFuncClass() because the
    // former returns the JVM-cached canonical String for the resolved Class object,
    // which makes ConcurrentHashMap.get() in MatrixTransportServerMetrics behave as
    // a hash + cheap equals (vs. a fresh String from the wire that defeats hashCode
    // caching on first access). Failure path (throw) is recorded via finally.
    boolean psfSuccess = false;
    long psfStartNanos = System.nanoTime();
    long responseBytes = 0L;
    try {
      PartitionGetResult partResult = func.partitionGet(request.getPartParam());
      ResponseData responseData = new GetUDFResponse(partResult);
      responseBytes = Math.max(0L, responseData.getDataLen());
      psfSuccess = true;

      if (metricEnable) {
        // Data buffer len set to 0 now
        AngelPSLoad metrics = new AngelPSLoad(
            context.getPs().getServerId().toString(), request.getGetFuncClass(),
            request.getPartParam().getPartKey().getPartitionId(),
            request.requestSize, responseData.getDataLen(), 0);
        context.getMetricsService().report(metrics);
      }

      return responseData;
    } finally {
      long psfHandleNanos = System.nanoTime() - psfStartNanos;
      rpcMetrics.recordPsf(funcClass.getName(), psfHandleNanos, request.requestSize,
          responseBytes, psfSuccess);
    }
  }

  @SuppressWarnings("unchecked")
  private static Constructor<? extends GetFunc> resolveConstructor(String className)
      throws ClassNotFoundException, NoSuchMethodException {
    Constructor<? extends GetFunc> cached = GET_FUNC_CTOR_CACHE.get(className);
    if (cached != null) {
      return cached;
    }

    Class<?> raw = Class.forName(className);
    if (!GetFunc.class.isAssignableFrom(raw)) {
      throw new IllegalArgumentException(className + " is not a GetFunc");
    }
    Constructor<? extends GetFunc> created =
        ((Class<? extends GetFunc>) raw).getDeclaredConstructor();
    created.setAccessible(true);
    Constructor<? extends GetFunc> raced = GET_FUNC_CTOR_CACHE.putIfAbsent(className, created);
    return raced != null ? raced : created;
  }
}
