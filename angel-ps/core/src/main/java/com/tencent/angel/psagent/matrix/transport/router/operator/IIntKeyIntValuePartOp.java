package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IIntKeyIntValuePartOp {
  int[] getKeys();
  int[] getValues();
  void add(int key, int value);
  void add(int[] keys, int[] values);
}
