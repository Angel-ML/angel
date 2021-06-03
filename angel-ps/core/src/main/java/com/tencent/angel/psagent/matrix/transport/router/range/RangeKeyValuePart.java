package com.tencent.angel.psagent.matrix.transport.router.range;

import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;

public abstract class RangeKeyValuePart extends KeyValuePart {
  /**
   * Start position for this partition in keys/values
   */
  protected final transient int startPos;

  /**
   * End position for this partition in keys/values
   */
  protected final transient int endPos;

  public RangeKeyValuePart(int rowId,int startPos, int endPos) {
    super(rowId);
    this.startPos = startPos;
    this.endPos  = endPos;
  }

  public RouterType getRouterType() {
    return RouterType.RANGE;
  }
}
