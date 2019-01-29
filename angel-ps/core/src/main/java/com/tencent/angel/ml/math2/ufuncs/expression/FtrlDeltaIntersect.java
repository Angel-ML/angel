package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

public class FtrlDeltaIntersect extends Binary {

  private double alpha;

  public FtrlDeltaIntersect(boolean inplace, double alpha) {
    setInplace(inplace);
    setKeepStorage(Constant.keepStorage);
    this.alpha = alpha;
  }

  @Override
  public OpType getOpType() {
    return OpType.INTERSECTION;
  }

  @Override
  public double apply(double ele1, double ele2) {
    return (Math.sqrt(ele1 * ele1 + ele2) - Math.sqrt(ele2)) / alpha;
  }

  @Override
  public double apply(double ele1, float ele2) {
    return (Math.sqrt(ele1 * ele1 + ele2) - Math.sqrt(ele2)) / alpha;
  }

  @Override
  public double apply(double ele1, long ele2) {
    return (Math.sqrt(ele1 * ele1 + ele2) - Math.sqrt(ele2)) / alpha;
  }

  @Override
  public double apply(double ele1, int ele2) {
    return (Math.sqrt(ele1 * ele1 + ele2) - Math.sqrt(ele2)) / alpha;
  }

  @Override
  public float apply(float ele1, float ele2) {
    return (float) ((Math.sqrt(ele1 * ele1 + ele2) - Math.sqrt(ele2)) / alpha);
  }

  @Override
  public float apply(float ele1, long ele2) {
    return (float) ((Math.sqrt(ele1 * ele1 + ele2) - Math.sqrt(ele2)) / alpha);
  }

  @Override
  public float apply(float ele1, int ele2) {
    return (float) ((Math.sqrt(ele1 * ele1 + ele2) - Math.sqrt(ele2)) / alpha);
  }

  @Override
  public long apply(long ele1, long ele2) {
    return (long) ((Math.sqrt(ele1 * ele1 + ele2) - Math.sqrt(ele2)) / alpha);
  }

  @Override
  public long apply(long ele1, int ele2) {
    return (long) ((Math.sqrt(ele1 * ele1 + ele2) - Math.sqrt(ele2)) / alpha);
  }

  @Override
  public int apply(int ele1, int ele2) {
    return (int) ((Math.sqrt(ele1 * ele1 + ele2) - Math.sqrt(ele2)) / alpha);
  }
}
