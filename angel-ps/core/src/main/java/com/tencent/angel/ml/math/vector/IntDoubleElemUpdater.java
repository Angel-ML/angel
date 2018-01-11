package com.tencent.angel.ml.math.vector;

public interface IntDoubleElemUpdater {
  double action(int index, double value, ElemUpdateParam param);
}
