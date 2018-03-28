package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.ml.matrix.transport.PSLocation;

public class ServerEvent extends DispatcherEvent{
  private final PSLocation psLoc;
  public ServerEvent(EventType type, PSLocation psLoc) {
    super(type);
    this.psLoc = psLoc;
  }

  public PSLocation getPsLoc() {
    return psLoc;
  }
}
