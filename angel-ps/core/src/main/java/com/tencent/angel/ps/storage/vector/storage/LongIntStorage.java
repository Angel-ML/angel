package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.op.ILongIntOp;

public abstract class LongIntStorage extends BasicTypeStorage implements ILongIntOp {

  public LongIntStorage(long indexOffset) {
    super(indexOffset);
  }

  public LongIntStorage() {
    this(0L);
  }
}
