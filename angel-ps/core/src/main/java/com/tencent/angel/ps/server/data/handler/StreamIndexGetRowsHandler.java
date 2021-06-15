package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.request.IndexPartGetRowsRequest;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.ps.server.data.response.IndexPartGetRowsResponse;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.ResponseHeader;
import com.tencent.angel.ps.server.data.response.ResponseType;
import com.tencent.angel.ps.storage.vector.ServerBasicTypeRow;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;
import com.tencent.angel.utils.MatrixUtils;
import com.tencent.angel.utils.ByteBufUtils;
import io.netty.buffer.ByteBuf;

public class StreamIndexGetRowsHandler extends StreamHandler {

  public StreamIndexGetRowsHandler(PSContext context) {
    super(context);
  }

  @Override
  public RequestData parseRequest(ByteBuf in) {
    IndexPartGetRowsRequest request = new IndexPartGetRowsRequest();
    request.deserializeHeader(in);
    request.setIn(in);
    return request;
  }

  @Override
  public ResponseData handle(RequestHeader requestHeader, RequestData data)  throws Exception {
    IndexPartGetRowsRequest request = (IndexPartGetRowsRequest) data;
    IndexPartGetRowsResponse response = new IndexPartGetRowsResponse();

    int[] rowIds = request.getRowIds();
    int rowNum = rowIds.length;

    ServerBasicTypeRow row0 = (ServerBasicTypeRow) context.getMatrixStorageManager()
        .getRow(requestHeader.matrixId, request.getRowIds()[0], requestHeader.partId);
    ValueType valueType = MatrixUtils.getValueType(row0.getRowType());

    ResponseHeader responseHeader = new ResponseHeader(requestHeader.seqId, requestHeader.methodId,
        context.getRunningContext().getState(), ResponseType.SUCCESS);

    ByteBuf in = request.getInputBuffer();

    // Read router type
    RouterType routerType = RouterType.valueOf(in.readInt());
    // Key type
    KeyType keyType = KeyType.valueOf(in.readInt());
    // Row id
    int rowId = in.readInt();
    // Key number
    int size = in.readInt();

    // Calculate final output buffer len
    // Response header
    int buffLen = responseHeader.bufferLen();
    // Data flag
    buffLen += ByteBufSerdeUtils.BOOLEN_LENGTH;

    // Value type
    buffLen += ByteBufSerdeUtils.INT_LENGTH;
    // Row id
    buffLen += ByteBufSerdeUtils.INT_LENGTH;
    // Size
    buffLen += ByteBufSerdeUtils.INT_LENGTH;
    //Data
    buffLen += size * ByteBufSerdeUtils.serializedValueLen(valueType);

    // Allocate result buffer
    ByteBuf resultBuf = ByteBufUtils.allocResultBuf(buffLen, context.isUseDirectBuffer());

    // Header
    responseHeader.serialize(resultBuf);
    ByteBufSerdeUtils.serializeBoolean(resultBuf, true);
    int colNum = 0;

    int markPos = in.readerIndex();
    for (int i = 0; i < rowNum; i++) {
      in.readerIndex(markPos);

      // Serialize Value part head
      ByteBufSerdeUtils.serializeInt(resultBuf, valueType.getTypeId());
      ByteBufSerdeUtils.serializeInt(resultBuf, rowIds[i]);
      ByteBufSerdeUtils.serializeInt(resultBuf, colNum);

      ServerBasicTypeRow row = (ServerBasicTypeRow) context.getMatrixStorageManager()
          .getRow(requestHeader.matrixId, rowIds[i], requestHeader.partId);
      resultBuf.writeInt(rowIds[i]);
      if (request.getFunc() == null) {
        row.startRead();
        try {
          row.indexGet(keyType, colNum, in, resultBuf, null);
        } finally {
          row.endRead();
        }
      } else {
        row.startWrite();
        try {
          row.indexGet(keyType, colNum, in, resultBuf, request.getFunc());
        } finally {
          row.endWrite();
        }
      }
    }

    //response.setOutputBuffer(resultBuf);
    return response;
  }
}
