package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.request.GetRowsSplitRequest;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.response.GetRowsSplitResponse;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.utils.MatrixUtils;
import io.netty.buffer.ByteBuf;

public class GetRowsHandler extends Handler {

  public GetRowsHandler(PSContext context) {
    super(context);
  }

  @Override
  public RequestData parseRequest(ByteBuf in) {
    GetRowsSplitRequest request = new GetRowsSplitRequest();
    request.deserialize(in);
    return request;
  }

  @Override
  public ResponseData handle(RequestHeader header, RequestData data) throws Exception {
    GetRowsSplitRequest request = (GetRowsSplitRequest) data;

    int[] rowIds = request.getRowIds();
    ServerRow[] rows = new ServerRow[rowIds.length];
    for(int i = 0; i < rowIds.length; i++) {
      rows[i] = MatrixUtils.getRow(context.getMatrixStorageManager(),
          header.matrixId, header.partId, rowIds[i]);
    }

    return new GetRowsSplitResponse(rows);
  }
}
