package com.tencent.angel.ml.math.vector;

public interface LongFloatElemUpdater {
  float action(long index, float value, ElemUpdateParam param);
}
