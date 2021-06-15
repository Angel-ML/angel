package com.tencent.angel.psagent.matrix.transport.router;

import com.tencent.angel.ps.storage.vector.element.IElement;

public interface IKeyHash {
  int hash(int key);
  int hash(long key);
  int hash(String key);
  int hash(IElement key);
}
