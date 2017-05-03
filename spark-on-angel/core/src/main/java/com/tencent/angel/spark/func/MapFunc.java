package com.tencent.angel.spark.func;

import com.tencent.angel.common.Serialize;

public interface MapFunc extends Serialize {
  double call(double value);
}
