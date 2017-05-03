package com.tencent.angel.spark.func;

import com.tencent.angel.common.Serialize;

public interface Zip2MapFunc extends Serialize {
  double call(double value1, double value2);
}
