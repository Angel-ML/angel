package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;

public abstract class RangeKeyPart extends KeyPart {
  /**
   * Start position for this partition in keys/values
   */
  protected final transient int startPos;

  /**
   * End position for this partition in keys/values
   */
  protected final transient int endPos;

  public RangeKeyPart(int rowId, int startPos, int endPos) {
    super(rowId);
    this.startPos = startPos;
    this.endPos = endPos;
  }

  public RouterType getRouterType() {
    return RouterType.RANGE;
  }

  public int getStartPos() {
    return startPos;
  }

  public int getEndPos() {
    return endPos;
  }
}
