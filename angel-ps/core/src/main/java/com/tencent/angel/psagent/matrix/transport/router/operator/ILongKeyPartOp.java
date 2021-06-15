package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface ILongKeyPartOp {
  long[] getKeys();
  void add(long key);
  void add(long[] keys);
}
