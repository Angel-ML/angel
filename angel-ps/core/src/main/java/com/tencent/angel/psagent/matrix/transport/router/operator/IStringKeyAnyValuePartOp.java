package com.tencent.angel.psagent.matrix.transport.router.operator;

import com.tencent.angel.ps.storage.vector.element.IElement;

public interface IStringKeyAnyValuePartOp {
  String[] getKeys();
  IElement[] getValues();
  void add(String key, IElement value);
  void add(String[] keys, IElement[] values);
}
