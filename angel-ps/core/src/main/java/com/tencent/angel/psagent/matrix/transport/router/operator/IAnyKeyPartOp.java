package com.tencent.angel.psagent.matrix.transport.router.operator;

import com.tencent.angel.ps.storage.vector.element.IElement;

public interface IAnyKeyPartOp {
  IElement[] getKeys();
  void add(IElement key);
  void add(IElement[] keys);
}
