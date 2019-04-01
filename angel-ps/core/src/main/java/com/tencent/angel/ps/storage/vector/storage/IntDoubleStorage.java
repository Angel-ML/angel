package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.op.IIntDoubleOp;

public abstract class IntDoubleStorage extends BasicTypeStorage implements IIntDoubleOp {

  public IntDoubleStorage(long indexOffset) {
    super(indexOffset);
  }

  public IntDoubleStorage() {
    this(0L);
  }
}
