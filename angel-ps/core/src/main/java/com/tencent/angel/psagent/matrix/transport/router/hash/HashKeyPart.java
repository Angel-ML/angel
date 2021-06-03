package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;

public abstract class HashKeyPart extends KeyPart {
  public HashKeyPart(int rowId) {
    super(rowId);
  }
  public RouterType getRouterType() {
    return RouterType.HASH;
  }
}
