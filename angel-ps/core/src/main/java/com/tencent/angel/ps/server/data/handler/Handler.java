package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.ps.PSContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class Handler implements IHandler {
  private static final Log LOG = LogFactory.getLog(Handler.class);
  protected final PSContext context;
  public Handler(PSContext context) {
    this.context = context;
  }
}
