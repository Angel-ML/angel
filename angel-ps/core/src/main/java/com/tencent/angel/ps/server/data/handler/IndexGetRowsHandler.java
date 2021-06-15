package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.request.IndexPartGetRowsRequest;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.response.IndexPartGetRowsResponse;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import io.netty.buffer.ByteBuf;

public class IndexGetRowsHandler extends Handler {

  public IndexGetRowsHandler(PSContext context) {
    super(context);
  }

  @Override
  public RequestData parseRequest(ByteBuf in) {
    IndexPartGetRowsRequest request = new IndexPartGetRowsRequest();
    request.deserialize(in);
    return request;
  }

  @Override
  public ResponseData handle(RequestHeader header, RequestData data) throws Exception {
    IndexPartGetRowsRequest request = (IndexPartGetRowsRequest) data;
    int[] rowIds = request.getRowIds();
    KeyPart keyPart = request.getKeyPart();

    ServerRow[] rows = new ServerRow[rowIds.length];
    ValuePart[] results = new ValuePart[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      rows[i] = context.getMatrixStorageManager()
          .getRow(header.matrixId, rowIds[i], header.partId);

      rows[i].startRead();
      try {
        results[i] = ServerRowUtils.getByKeys(rows[i], keyPart);
      } finally {
        rows[i].endRead();
      }
    }

    IndexPartGetRowsResponse response = new IndexPartGetRowsResponse(results);
    return response;
  }
}
