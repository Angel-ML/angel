package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.op.ILongLongOp;

public abstract class LongLongStorage extends BasicTypeStorage implements ILongLongOp {

  public LongLongStorage(long indexOffset) {
    super(indexOffset);
  }

  public LongLongStorage() {
    this(0L);
  }
}
