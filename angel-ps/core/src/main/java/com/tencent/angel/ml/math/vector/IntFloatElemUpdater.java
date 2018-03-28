package com.tencent.angel.ml.math.vector;

public interface IntFloatElemUpdater {
  float action(int index, float value, ElemUpdateParam param);
}
