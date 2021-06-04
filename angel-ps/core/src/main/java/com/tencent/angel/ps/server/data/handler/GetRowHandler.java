package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.request.GetRowSplitRequest;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.response.GetRowSplitResponse;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.utils.MatrixUtils;
import io.netty.buffer.ByteBuf;

public class GetRowHandler extends Handler {

  public GetRowHandler(PSContext context) {
    super(context);
  }

  @Override
  public RequestData parseRequest(ByteBuf in) {
    GetRowSplitRequest request = new GetRowSplitRequest();
    request.deserialize(in);
    return request;
  }

  @Override
  public ResponseData handle(RequestHeader header, RequestData data) throws Exception {
    GetRowSplitRequest request = (GetRowSplitRequest) data;
    ServerRow row = MatrixUtils.getRow(context.getMatrixStorageManager(), header.matrixId,
        header.partId, request.getRowId());
    return new GetRowSplitResponse(row);
  }
}
