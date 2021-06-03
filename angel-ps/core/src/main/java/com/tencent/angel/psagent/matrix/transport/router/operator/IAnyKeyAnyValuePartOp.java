package com.tencent.angel.psagent.matrix.transport.router.operator;

import com.tencent.angel.ps.storage.vector.element.IElement;

public interface IAnyKeyAnyValuePartOp {
  IElement[] getKeys();
  IElement[] getValues();
  void add(IElement key, IElement value);
  void add(IElement[] keys, IElement[] values);
}
