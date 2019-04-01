package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.op.ILongElementOp;

public abstract class LongElementStorage extends ObjectTypeStorage implements ILongElementOp {

  public LongElementStorage(Class<? extends IElement> objectClass, long indexOffset) {
    super(objectClass, indexOffset);
  }

  public LongElementStorage() {
    super(null, 0L);
  }
}
