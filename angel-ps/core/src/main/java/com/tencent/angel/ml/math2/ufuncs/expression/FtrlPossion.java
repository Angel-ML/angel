package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

import java.util.Random;

public class FtrlPossion extends Binary {

  Random random;
  double e = 10e-8;
  float p;

  public FtrlPossion(boolean inplace, float p) {
    random = new Random(System.currentTimeMillis());
    setInplace(inplace);
    setKeepStorage(Constant.keepStorage);
    this.p = p;
  }

  @Override
  public OpType getOpType() {
    return OpType.INTERSECTION;
  }

  @Override
  public double apply(double ele1, double ele2) {
    // not the first time
    if (ele1 > e)
      return ele2;
    // first time and do the sample
    if (ele1 < e && random.nextFloat() < p)
      return ele2;
    // set to zero
    return 0.0;
  }

  @Override
  public double apply(double ele1, float ele2) {
    // not the first time
    if (ele1 > e)
      return ele2;
    // first time and do the sample
    if (ele1 < e && random.nextFloat() < p)
      return ele2;
    // set to zero
    return 0.0;
  }

  @Override
  public double apply(double ele1, long ele2) {
    // not the first time
    if (ele1 > e)
      return ele2;
    // first time and do the sample
    if (ele1 < e && random.nextFloat() < p)
      return ele2;
    // set to zero
    return 0.0;
  }

  @Override
  public double apply(double ele1, int ele2) {
    // not the first time
    if (ele1 > e)
      return ele2;
    // first time and do the sample
    if (ele1 < e && random.nextFloat() < p)
      return ele2;
    // set to zero
    return 0.0;
  }

  @Override
  public float apply(float ele1, float ele2) {
    // not the first time
    if (ele1 > e)
      return ele2;
    // first time and do the sample
    if (ele1 < e && random.nextFloat() < p)
      return ele2;
    // set to zero
    return 0.0f;
  }

  @Override
  public float apply(float ele1, long ele2) {
    // not the first time
    if (ele1 > e)
      return ele2;
    // first time and do the sample
    if (ele1 < e && random.nextFloat() < p)
      return ele2;
    // set to zero
    return 0.0f;
  }

  @Override
  public float apply(float ele1, int ele2) {
    // not the first time
    if (ele1 > e)
      return ele2;
    // first time and do the sample
    if (ele1 < e && random.nextFloat() < p)
      return ele2;
    // set to zero
    return 0.0f;
  }

  @Override
  public long apply(long ele1, long ele2) {
    // not the first time
    if (ele1 > e)
      return ele2;
    // first time and do the sample
    if (ele1 < e && random.nextFloat() < p)
      return ele2;
    // set to zero
    return 0L;
  }

  @Override
  public long apply(long ele1, int ele2) {
    // not the first time
    if (ele1 > e)
      return ele2;
    // first time and do the sample
    if (ele1 < e && random.nextFloat() < p)
      return ele2;
    // set to zero
    return 0L;
  }

  @Override
  public int apply(int ele1, int ele2) {
    // not the first time
    if (ele1 > e)
      return ele2;
    // first time and do the sample
    if (ele1 < e && random.nextFloat() < p)
      return ele2;
    // set to zero
    return 0;
  }
}
