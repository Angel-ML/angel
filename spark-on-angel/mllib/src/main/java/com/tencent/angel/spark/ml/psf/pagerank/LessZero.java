package com.tencent.angel.spark.ml.psf.pagerank;

import com.tencent.angel.ml.math2.ufuncs.expression.Unary;

public class LessZero extends Unary {

  private float threshold;

  public LessZero(boolean inplace, float threshold) {
    setInplace(inplace);
    this.threshold = threshold;
  }

  @Override
  public double apply(double elem) {
    if (elem < threshold) return 0;
    return elem;
  }

  @Override
  public float apply(float elem) {
    if (elem < threshold) return 0;
    return elem;
  }

  @Override
  public long apply(long elem) {
    if (elem < threshold) return 0;
    return elem;
  }

  @Override
  public int apply(int elem) {
    if (elem < threshold) return 0;
    return elem;
  }
}
