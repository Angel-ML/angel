package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.metrics.AngelPSLoad;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.WorkerPool;
import com.tencent.angel.ps.server.data.profiling.MatrixTransportServerMetrics;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.request.UpdateUDFRequest;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.UpdateUDFResponse;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PSFUpdateHandler extends Handler {
  private static final Log LOG = LogFactory.getLog(WorkerPool.class);
  private static final ConcurrentHashMap<String, Constructor<? extends UpdateFunc>> UPDATE_FUNC_CTOR_CACHE =
      new ConcurrentHashMap<>();
  private final boolean metricEnable;
  /** See PSFGetHandler#rpcMetrics for rationale. */
  private final MatrixTransportServerMetrics rpcMetrics =
      MatrixTransportServerMetrics.getInstance();

  public PSFUpdateHandler(PSContext context) {
    super(context);
    this.metricEnable = context.getMetricsEnable();
  }

  @Override
  public RequestData parseRequest(ByteBuf in) {
    UpdateUDFRequest request = new UpdateUDFRequest();
    request.deserialize(in);
    return request;
  }

  @Override
  public ResponseData handle(RequestHeader header, RequestData data) throws Exception {
    UpdateUDFRequest request = (UpdateUDFRequest)data;
    Constructor<? extends UpdateFunc> constructor = getOrCreateConstructor(request.getUpdaterFuncClass());
    UpdateFunc func = constructor.newInstance();
    Class<? extends UpdateFunc> funcClass = constructor.getDeclaringClass();
    context.setTryCount(header.getTryCount());
    func.setPsContext(context);

    // Per-PSF-class metrics: time only the partitionUpdate body. See PSFGetHandler
    // for the rationale on funcClass.getName() vs request.getUpdaterFuncClass().
    boolean psfSuccess = false;
    long psfStartNanos = System.nanoTime();
    try {
      func.partitionUpdate(request.getPartParam());
      psfSuccess = true;
    } finally {
      long psfHandleNanos = System.nanoTime() - psfStartNanos;
      // Update PSFs do not return a body; responseBytes = 0.
      rpcMetrics.recordPsf(funcClass.getName(), psfHandleNanos, request.requestSize, 0L,
          psfSuccess);
    }

    if (metricEnable) {
      // Data buffer len set to 0 now
      AngelPSLoad metrics = new AngelPSLoad(
          context.getPs().getServerId().toString(), request.getUpdaterFuncClass(),
          request.getPartParam().getPartKey().getPartitionId(),
          request.requestSize, 0, 0);
      context.getMetricsService().report(metrics);
    }

    return new UpdateUDFResponse();

  }

  @SuppressWarnings("unchecked")
  private static Constructor<? extends UpdateFunc> getOrCreateConstructor(String className)
      throws ClassNotFoundException, NoSuchMethodException {
    Constructor<? extends UpdateFunc> cached = UPDATE_FUNC_CTOR_CACHE.get(className);
    if (cached != null) {
      return cached;
    }

    Class<?> raw = Class.forName(className);
    if (!UpdateFunc.class.isAssignableFrom(raw)) {
      throw new IllegalArgumentException(className + " is not an UpdateFunc");
    }
    Constructor<? extends UpdateFunc> created =
        ((Class<? extends UpdateFunc>) raw).getDeclaredConstructor();
    created.setAccessible(true);
    Constructor<? extends UpdateFunc> raced = UPDATE_FUNC_CTOR_CACHE.putIfAbsent(className, created);
    return raced != null ? raced : created;
  }
}
