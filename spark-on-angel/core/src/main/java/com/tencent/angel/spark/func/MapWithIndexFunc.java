package com.tencent.angel.spark.func;

import com.tencent.angel.common.Serialize;

public interface MapWithIndexFunc extends Serialize {
  double call(int index, double value);
}
