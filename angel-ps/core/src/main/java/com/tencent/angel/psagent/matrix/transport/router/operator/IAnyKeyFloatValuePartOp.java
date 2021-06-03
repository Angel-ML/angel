package com.tencent.angel.psagent.matrix.transport.router.operator;

import com.tencent.angel.ps.storage.vector.element.IElement;

public interface IAnyKeyFloatValuePartOp {
  IElement[] getKeys();
  float[] getValues();
  void add(IElement key, float value);
  void add(IElement[] keys, float[] values);
}
