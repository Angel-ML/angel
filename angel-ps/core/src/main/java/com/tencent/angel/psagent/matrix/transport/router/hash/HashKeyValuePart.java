package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;

public abstract class HashKeyValuePart extends KeyValuePart {
  public HashKeyValuePart(int rowId) {
    super(rowId);
  }

  public RouterType getRouterType() {
    return RouterType.HASH;
  }
}
