package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IIntKeyPartOp {
  int[] getKeys();
  void add(int key);
  void add(int[] keys);
}
