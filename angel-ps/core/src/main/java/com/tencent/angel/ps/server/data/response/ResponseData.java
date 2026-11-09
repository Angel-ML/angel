package com.tencent.angel.ps.server.data.response;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.server.data.request.RequestData;

public abstract class ResponseData implements Serialize {
  private transient volatile int dataLen = -1;
  protected transient volatile RequestData requestData;

  public int getDataLen() {
    if(dataLen < 0) {
      dataLen = bufferLen();
    }
    return dataLen;
  }

  public RequestData getRequestData() {
    return requestData;
  }

  public void setRequestData(RequestData requestData) {
    this.requestData = requestData;
  }
}
