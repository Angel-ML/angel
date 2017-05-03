package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ml.matrix.udf.aggr.AggrResult;

public class ScalarAggrResult extends AggrResult {
  private final double result;

  public ScalarAggrResult(double result) {
    this.result = result;
  }

  public double getResult() {
    return result;
  }
}
