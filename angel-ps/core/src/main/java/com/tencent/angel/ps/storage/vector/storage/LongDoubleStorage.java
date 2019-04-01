package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.op.ILongDoubleOp;

public abstract class LongDoubleStorage extends BasicTypeStorage implements ILongDoubleOp {

  public LongDoubleStorage(long indexOffset) {
    super(indexOffset);
  }

  public LongDoubleStorage() {
    this(0L);
  }
}
