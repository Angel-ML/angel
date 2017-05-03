package com.tencent.angel.spark.func;

import com.tencent.angel.common.Serialize;

public interface Zip2MapWithIndexFunc extends Serialize {
   double call(int index, double value1, double value2);
}
