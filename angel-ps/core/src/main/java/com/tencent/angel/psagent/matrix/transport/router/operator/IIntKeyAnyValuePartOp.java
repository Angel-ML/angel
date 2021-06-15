package com.tencent.angel.psagent.matrix.transport.router.operator;

import com.tencent.angel.ps.storage.vector.element.IElement;

public interface IIntKeyAnyValuePartOp {
  int[] getKeys();
  IElement[] getValues();
  void add(int key, IElement value);
  void add(int[] keys, IElement[] values);
}
