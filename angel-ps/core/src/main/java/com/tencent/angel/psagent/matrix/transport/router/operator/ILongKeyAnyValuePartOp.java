package com.tencent.angel.psagent.matrix.transport.router.operator;

import com.tencent.angel.ps.storage.vector.element.IElement;

public interface ILongKeyAnyValuePartOp {
  long[] getKeys();
  IElement[] getValues();
  void add(long key, IElement value);
  void add(long[] keys, IElement[] values);
}
