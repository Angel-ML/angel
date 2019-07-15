package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

public class IndexGet extends Binary {

  public IndexGet(boolean inplace) {
    setInplace(inplace);
    setKeepStorage(true);
  }

  @Override
  public OpType getOpType() {
    return OpType.INTERSECTION;
  }

  @Override
  public double apply(double ele1, double ele2) {
    return ele1;
  }

  @Override
  public double apply(double ele1, float ele2) {
    return ele1;
  }

  @Override
  public double apply(double ele1, long ele2) {
    return ele1;
  }

  @Override
  public double apply(double ele1, int ele2) {
    return ele1;
  }

  @Override
  public float apply(float ele1, float ele2) {
    return ele1;
  }

  @Override
  public float apply(float ele1, long ele2) {
    return ele1;
  }

  @Override
  public float apply(float ele1, int ele2) {
    return ele1;
  }

  @Override
  public long apply(long ele1, long ele2) {
    return ele1;
  }

  @Override
  public long apply(long ele1, int ele2) {
    return ele1;
  }

  @Override
  public int apply(int ele1, int ele2) {
    return ele1;
  }
}
