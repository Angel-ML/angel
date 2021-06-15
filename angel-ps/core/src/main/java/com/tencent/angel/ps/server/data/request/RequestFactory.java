package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.ps.server.data.TransportMethod;

public class RequestFactory {

  public static RequestData createEmptyRequestData(TransportMethod method) {
    switch (method) {
      case INDEX_GET_ROW:
        return new IndexPartGetRowRequest();

      case INDEX_GET_ROWS:
        return new IndexPartGetRowsRequest();

      case GET_ROWSPLIT:
        return new GetRowSplitRequest();

      case GET_ROWSSPLIT:
        return new GetRowsSplitRequest();

      case GET_PSF:
        return new GetUDFRequest();

      case UPDATE_PSF:
        return new UpdateUDFRequest();

      case UPDATE:
        return new UpdateRequest();

      default:
        throw new UnsupportedOperationException("Unsupport request type " + method);
    }
  }
}
