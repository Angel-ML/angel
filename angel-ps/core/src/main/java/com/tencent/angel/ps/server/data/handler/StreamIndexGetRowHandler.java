package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.server.data.request.IndexPartGetRowRequest;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.request.ValueType;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.ResponseHeader;
import com.tencent.angel.ps.server.data.response.ResponseType;
import com.tencent.angel.ps.server.data.response.StreamIndexPartGetRowResponse;
import com.tencent.angel.ps.storage.vector.ServerBasicTypeRow;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;
import com.tencent.angel.utils.ByteBufUtils;
import com.tencent.angel.utils.MatrixUtils;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class StreamIndexGetRowHandler extends StreamHandler {

  private static final Log LOG = LogFactory.getLog(StreamIndexGetRowHandler.class);

  public StreamIndexGetRowHandler(PSContext context) {
    super(context);
  }

  @Override
  public RequestData parseRequest(ByteBuf in) {
    IndexPartGetRowRequest request = new IndexPartGetRowRequest();
    request.deserializeHeader(in);
    request.setIn(in);
    return request;
  }

  @Override
  public ResponseData handle(RequestHeader requestHeader, RequestData data) throws Exception {
    IndexPartGetRowRequest request = (IndexPartGetRowRequest) data;
    StreamIndexPartGetRowResponse response = new StreamIndexPartGetRowResponse();
    ByteBuf in = request.getInputBuffer();

    ServerBasicTypeRow row = (ServerBasicTypeRow) context.getMatrixStorageManager()
        .getRow(requestHeader.matrixId, request.getRowId(), requestHeader.partId);
    ValueType valueType = MatrixUtils.getValueType(row.getRowType());

    ResponseHeader responseHeader = new ResponseHeader(requestHeader.seqId, requestHeader.methodId,
        context.getRunningContext().getState(), ResponseType.SUCCESS);

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

    // Allocate final result byte buffer
    ByteBuf resultBuf = ByteBufUtils.allocResultBuf(buffLen, context.isUseDirectBuffer());

    // Write response header
    responseHeader.serialize(resultBuf);

    // Data
    // Value part flag
    ByteBufSerdeUtils.serializeBoolean(resultBuf, true);
    // Value type
    ByteBufSerdeUtils.serializeInt(resultBuf, valueType.getTypeId());
    // Row id
    ByteBufSerdeUtils.serializeInt(resultBuf, rowId);
    // Values number
    ByteBufSerdeUtils.serializeInt(resultBuf, size);
    // Result data
    if (request.getFunc() == null) {
      row.startRead();
      try {
        row.indexGet(keyType, size, in, resultBuf, null);
      } finally {
        row.endRead();
      }
    } else {
      row.startWrite();
      try {
        row.indexGet(keyType, size, in, resultBuf, request.getFunc());
      } finally {
        row.endWrite();
      }
    }
    response.setOutputBuffer(resultBuf);
    return response;
  }
}
