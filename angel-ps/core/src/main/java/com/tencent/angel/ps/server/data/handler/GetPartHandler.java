package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.request.GetPartitionRequest;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.response.GetPartitionResponse;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.utils.MatrixUtils;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GetPartHandler extends Handler {
  private static final Log LOG = LogFactory.getLog(GetPartHandler.class);
  public GetPartHandler(PSContext context) {
    super(context);
  }

  @Override
  public RequestData parseRequest(ByteBuf in) {
    GetPartitionRequest request = new GetPartitionRequest();
    request.deserialize(in);
    return request;
  }

  @Override
  public ResponseData handle(RequestHeader header, RequestData data) throws Exception {
    GetPartitionRequest request = (GetPartitionRequest)data;
    ServerPartition partition = MatrixUtils.getPart(context.getMatrixStorageManager(),
        header.matrixId, header.partId);
    return new GetPartitionResponse(partition);
  }
}
