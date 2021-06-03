package com.tencent.angel.psagent.matrix.transport.router.operator;

import com.tencent.angel.ps.storage.vector.element.IElement;

public interface IAnyKeyIntValuePartOp {
  IElement[] getKeys();
  int[] getValues();
  void add(IElement key, int value);
  void add(IElement[] keys, int[] values);
}
