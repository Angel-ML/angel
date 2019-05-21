package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

public class MomentUpdate extends Binary {

  private double momentum;
  private double eta;

  public MomentUpdate(boolean inplace, double momentum, double eta) {
    setInplace(inplace);
    setKeepStorage(Constant.keepStorage);
    this.momentum = momentum;
    this.eta = eta;
  }

  @Override
  public OpType getOpType() {
    return OpType.UNION;
  }

  @Override
  public double apply(double ele1, double ele2) {
    return ele1 * momentum + ele2 * eta;
  }

  @Override
  public double apply(double ele1, float ele2) {
    return ele1 * momentum + ele2 * eta;
  }

  @Override
  public double apply(double ele1, long ele2) {
    return ele1 * momentum + ele2 * eta;
  }

  @Override
  public double apply(double ele1, int ele2) {
    return ele1 * momentum + ele2 * eta;
  }

  @Override
  public float apply(float ele1, float ele2) {
    return (float) (ele1 * momentum + ele2 * eta);
  }

  @Override
  public float apply(float ele1, long ele2) {
    return (float) (ele1 * momentum + ele2 * eta);
  }

  @Override
  public float apply(float ele1, int ele2) {
    return (float) (ele1 * momentum + ele2 * eta);
  }

  @Override
  public long apply(long ele1, long ele2) {
    return (long) (ele1 * momentum + ele2 * eta);
  }

  @Override
  public long apply(long ele1, int ele2) {
    return (long) (ele1 * momentum + ele2 * eta);
  }

  @Override
  public int apply(int ele1, int ele2) {
    return (int) (ele1 * momentum + ele2 * eta);
  }
}
