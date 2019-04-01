package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.op.IIntLongOp;

public abstract class IntLongStorage extends BasicTypeStorage implements IIntLongOp {

  public IntLongStorage(long indexOffset) {
    super(indexOffset);
  }

  public IntLongStorage() {
    this(0L);
  }
}
