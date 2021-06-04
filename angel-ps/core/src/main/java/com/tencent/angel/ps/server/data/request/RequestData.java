package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.common.Serialize;

public abstract class RequestData implements Serialize {

  /**
   * request size
   */
  public long requestSize;
  
}
