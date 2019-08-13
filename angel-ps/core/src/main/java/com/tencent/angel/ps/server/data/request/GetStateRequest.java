package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.TransportMethod;

public class GetStateRequest extends PSRequest {

  public GetStateRequest(ParameterServerId psId) {
    super(-1, psId);
  }

  public GetStateRequest() {
    this(null);
  }

  @Override
  public int getEstimizeDataSize() {
    return 0;
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.GET_STATE;
  }

}
