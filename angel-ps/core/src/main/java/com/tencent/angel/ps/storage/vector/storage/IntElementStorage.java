package com.tencent.angel.ps.storage.vector.storage;


import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.op.IIntElementOp;

public abstract class IntElementStorage extends ObjectTypeStorage implements IIntElementOp {
  public IntElementStorage(Class<? extends IElement> objectClass, long indexOffset) {
    super(objectClass, indexOffset);
    this.objectClass = objectClass;
  }

  public IntElementStorage() {
    this(null, 0L);
  }
}
