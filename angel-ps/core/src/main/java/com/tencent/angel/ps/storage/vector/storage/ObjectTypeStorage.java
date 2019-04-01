package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

public abstract class ObjectTypeStorage extends Storage {
  protected Class<? extends IElement> objectClass;
  public ObjectTypeStorage(Class<? extends IElement> objectClass, long indexOffset) {
    super(indexOffset);
    this.objectClass = objectClass;
  }

  protected IElement newElement() {
    try {
      return objectClass.newInstance();
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    throw new UnsupportedOperationException("pipeline add/set is not support for complex type now");
  }
}
