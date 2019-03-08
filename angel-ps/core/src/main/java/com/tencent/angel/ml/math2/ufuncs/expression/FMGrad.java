package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

public class FMGrad extends Binary {

  private double dot;

  public FMGrad(boolean inplace, double dot) {
    setInplace(inplace);
    setKeepStorage(Constant.keepStorage);
    this.dot = dot;
  }

  @Override
  public OpType getOpType() {
    return OpType.INTERSECTION;
  }

  @Override
  public double apply(double ele1, double ele2) {
    return ele1 * dot - ele2 * ele1 * ele1;
  }

  @Override
  public double apply(double ele1, float ele2) {
    return ele1 * dot - ele2 * ele1 * ele1;
  }

  @Override
  public double apply(double ele1, long ele2) {
    return ele1 * dot - ele2 * ele1 * ele1;
  }

  @Override
  public double apply(double ele1, int ele2) {
    return ele1 * dot - ele2 * ele1 * ele1;
  }

  @Override
  public float apply(float ele1, float ele2) {
    return (float) (ele1 * dot - ele2 * ele1 * ele1);
  }

  @Override
  public float apply(float ele1, long ele2) {
    return (float) (ele1 * dot - ele2 * ele1 * ele1);
  }

  @Override
  public float apply(float ele1, int ele2) {
    return (float) (ele1 * dot - ele2 * ele1 * ele1);
  }

  @Override
  public long apply(long ele1, long ele2) {
    return (long) (ele1 * dot - ele2 * ele1 * ele1);
  }

  @Override
  public long apply(long ele1, int ele2) {
    return (long) (ele1 * dot - ele2 * ele1 * ele1);
  }

  @Override
  public int apply(int ele1, int ele2) {
    return (int) (ele1 * dot - ele2 * ele1 * ele1);
  }
}
