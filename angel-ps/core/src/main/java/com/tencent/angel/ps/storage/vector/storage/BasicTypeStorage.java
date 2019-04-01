package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.op.BasicTypePipelineOp;

public abstract class BasicTypeStorage extends Storage implements BasicTypePipelineOp {

  public BasicTypeStorage(long indexOffset) {
    super(indexOffset);
  }
  public BasicTypeStorage() {
    this(0L);
  }
}
