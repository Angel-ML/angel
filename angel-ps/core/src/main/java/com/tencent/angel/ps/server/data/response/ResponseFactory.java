package com.tencent.angel.ps.server.data.response;

import com.tencent.angel.ps.server.data.TransportMethod;

public class ResponseFactory {

  public static ResponseData createEmptyResponseData(TransportMethod method) {
    switch (method) {
      case INDEX_GET_ROW:
        return new StreamIndexPartGetRowResponse();

      case INDEX_GET_ROWS:
        return new IndexPartGetRowsResponse();

      case GET_ROWSPLIT:
        return new GetRowSplitResponse();

      case GET_ROWSSPLIT:
        return new GetRowsSplitResponse();

      case GET_PSF:
        return new GetUDFResponse();

      case UPDATE_PSF:
        return new UpdateUDFResponse();

      case UPDATE:
        return new UpdateResponse();

      default:
        throw new UnsupportedOperationException("Unsupport request type " + method);
    }
  }
}
