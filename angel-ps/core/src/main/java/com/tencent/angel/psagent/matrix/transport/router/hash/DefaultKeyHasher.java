package com.tencent.angel.psagent.matrix.transport.router.hash;

import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.KeyHash;

public class DefaultKeyHasher extends KeyHash {

  public DefaultKeyHasher() {
  }

  @Override
  public int hash(int key) {
    return Math.abs(key);
  }

  @Override
  public int hash(long key) {
    return Math.abs((int)key);
  }

  @Override
  public int hash(String key) {
    return Math.abs(key.hashCode());
  }

  @Override
  public int hash(IElement key) {
    return Math.abs(key.hashCode());
  }
}
