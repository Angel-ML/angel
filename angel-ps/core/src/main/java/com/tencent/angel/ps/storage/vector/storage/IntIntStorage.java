package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.op.IIntIntOp;

public abstract class IntIntStorage extends BasicTypeStorage implements IIntIntOp {

  public IntIntStorage(long indexOffset) {
    super(indexOffset);
  }

  public IntIntStorage() {
    this(0L);
  }
}
