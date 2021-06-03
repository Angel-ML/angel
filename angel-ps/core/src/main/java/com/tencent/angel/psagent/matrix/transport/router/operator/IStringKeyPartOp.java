package com.tencent.angel.psagent.matrix.transport.router.operator;

public interface IStringKeyPartOp {
  String[] getKeys();
  void add(String key);
  void add(String[] keys);
}
