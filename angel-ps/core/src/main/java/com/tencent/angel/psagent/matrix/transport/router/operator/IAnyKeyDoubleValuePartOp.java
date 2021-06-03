package com.tencent.angel.psagent.matrix.transport.router.operator;

import com.tencent.angel.ps.storage.vector.element.IElement;

public interface IAnyKeyDoubleValuePartOp {
  IElement[] getKeys();
  double[] getValues();
  void add(IElement key, double value);
  void add(IElement[] keys, double[] values);
}
