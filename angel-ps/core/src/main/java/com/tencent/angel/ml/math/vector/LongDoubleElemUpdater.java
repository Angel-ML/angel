package com.tencent.angel.ml.math.vector;

public interface LongDoubleElemUpdater {
  double action(long index, double value, ElemUpdateParam param);
}
