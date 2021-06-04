package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.request.IndexPartGetRowRequest;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.StreamIndexPartGetRowResponse;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import com.tencent.angel.utils.MatrixUtils;
import io.netty.buffer.ByteBuf;

public class IndexGetRowHandler extends Handler {

  public IndexGetRowHandler(PSContext context) {
    super(context);
  }

  @Override
  public RequestData parseRequest(ByteBuf in) {
    IndexPartGetRowRequest request = new IndexPartGetRowRequest();
    request.deserialize(in);
    return request;
  }

  @Override
  public ResponseData handle(RequestHeader header, RequestData data) throws Exception {
    IndexPartGetRowRequest request = (IndexPartGetRowRequest) data;
    KeyPart keyPart = request.getKeyPart();

    ServerRow row = MatrixUtils.getRow(context.getMatrixStorageManager(), header.matrixId,
        header.partId, request.getRowId());

    ValuePart result;
    StreamIndexPartGetRowResponse response;

    row.startRead();
    try {
      result = ServerRowUtils.getByKeys(row, keyPart);
      response = new StreamIndexPartGetRowResponse(result);
      return response;
    } finally {
      row.endRead();
    }
  }
}
