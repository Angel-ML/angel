package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.WorkerPool;
import com.tencent.angel.ps.server.data.request.GetUDFRequest;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.response.GetUDFResponse;
import com.tencent.angel.ps.server.data.response.ResponseData;
import io.netty.buffer.ByteBuf;
import java.lang.reflect.Constructor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PSFGetHandler extends Handler {
  private static final Log LOG = LogFactory.getLog(WorkerPool.class);

  public PSFGetHandler(PSContext context) {
    super(context);
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
    Class<? extends GetFunc> funcClass =
        (Class<? extends GetFunc>) Class.forName(request.getGetFuncClass());
    Constructor<? extends GetFunc> constructor = funcClass.getConstructor();
    constructor.setAccessible(true);
    GetFunc func = constructor.newInstance();
    //LOG.info("Get PSF func = " + func.getClass().getName());
    func.setPsContext(context);
    PartitionGetResult partResult = func.partitionGet(request.getPartParam());
    return new GetUDFResponse(partResult);
  }
}
