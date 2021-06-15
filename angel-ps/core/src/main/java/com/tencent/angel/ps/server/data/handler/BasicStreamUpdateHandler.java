package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.request.UpdateRequest;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.UpdateResponse;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.utils.MatrixUtils;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BasicStreamUpdateHandler extends Handler {
  private static final Log LOG = LogFactory.getLog(BasicStreamUpdateHandler.class);
  public BasicStreamUpdateHandler(PSContext context) {
    super(context);
  }

  @Override
  public RequestData parseRequest(ByteBuf in) {
    UpdateRequest request = new UpdateRequest();
    request.deserializeHeader(in);
    return request;
  }

  @Override
  public ResponseData handle(RequestHeader header, RequestData data) throws Exception {
    UpdateRequest request = (UpdateRequest) data;
    ServerPartition part = MatrixUtils.getPart(context.getMatrixStorageManager(),
        header.matrixId, header.partId);

    ByteBuf in = request.getInputBuffer();

    // Filter comp key value
    ByteBufSerdeUtils.deserializeBoolean(in);

    part.update(in, request.getOp());
    return new UpdateResponse();
  }
}
