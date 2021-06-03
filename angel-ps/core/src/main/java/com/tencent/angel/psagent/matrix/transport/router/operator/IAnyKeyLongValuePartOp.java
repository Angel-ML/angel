package com.tencent.angel.psagent.matrix.transport.router.operator;

import com.tencent.angel.ps.storage.vector.element.IElement;

public interface IAnyKeyLongValuePartOp {
  IElement[] getKeys();
  long[] getValues();
  void add(IElement key, long value);
  void add(IElement[] keys, long[] values);
}
