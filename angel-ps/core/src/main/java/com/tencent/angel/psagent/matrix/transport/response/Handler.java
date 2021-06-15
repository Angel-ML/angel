package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.psagent.PSAgentContext;

public abstract class Handler implements IHandler {
  protected PSAgentContext context;
  public Handler(PSAgentContext context) {
    this.context = context;
  }
}
