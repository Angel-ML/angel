package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.WorkerPool;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.request.UpdateUDFRequest;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.UpdateUDFResponse;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Constructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PSFUpdateHandler extends Handler {
  private static final Log LOG = LogFactory.getLog(WorkerPool.class);

  public PSFUpdateHandler(PSContext context) {
    super(context);
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
    Class<? extends UpdateFunc> funcClass =
        (Class<? extends UpdateFunc>) Class.forName(request.getUpdaterFuncClass());
    Constructor<? extends UpdateFunc> constructor = funcClass.getConstructor();
    constructor.setAccessible(true);
    UpdateFunc func = constructor.newInstance();
    func.setPsContext(context);
    func.partitionUpdate(request.getPartParam());
    return new UpdateUDFResponse();
  }
}
