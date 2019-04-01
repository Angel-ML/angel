package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.op.IIntFloatOp;

public abstract class IntFloatStorage extends BasicTypeStorage implements IIntFloatOp {

  public IntFloatStorage(long indexOffset) {
    super(indexOffset);
  }

  public IntFloatStorage() {
    this(0L);
  }
}
