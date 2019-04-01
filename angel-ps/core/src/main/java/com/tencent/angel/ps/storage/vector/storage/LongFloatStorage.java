package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.op.ILongFloatOp;

public abstract class LongFloatStorage extends BasicTypeStorage implements ILongFloatOp {

  public LongFloatStorage(long indexOffset) {
    super(indexOffset);
  }

  public LongFloatStorage() {
    this(0L);
  }
}
