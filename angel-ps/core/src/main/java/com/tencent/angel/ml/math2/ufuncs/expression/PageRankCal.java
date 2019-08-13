package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

public class PageRankCal extends Binary {

  private float initRank;
  private float resetProb;

  public PageRankCal(boolean inplace, float initRank, float resetProb) {
    setInplace(inplace);
    setKeepStorage(Constant.keepStorage);
    this.initRank = initRank;
    this.resetProb = resetProb;
  }

  @Override
  public OpType getOpType() {
    return OpType.UNION;
  }

  @Override
  public double apply(double ele1, double ele2) {
    if (ele1 == 0.0 && ele2 != 0.0)
      return initRank + ele2 * (1 - resetProb);
    else if (ele2 != 0.0)
      return ele1 + ele2 * (1 - resetProb);
    else
      return ele1;
  }

  @Override
  public double apply(double ele1, float ele2) {
    if (ele1 == 0.0 && ele2 != 0.0)
      return initRank + ele2 * (1 - resetProb);
    else if (ele2 != 0.0)
      return ele1 + ele2 * (1 - resetProb);
    else
      return ele1;
  }

  @Override
  public double apply(double ele1, long ele2) {
    if (ele1 == 0.0 && ele2 != 0.0)
      return initRank + ele2 * (1 - resetProb);
    else if (ele2 != 0.0)
      return ele1 + ele2 * (1 - resetProb);
    else
      return ele1;
  }

  @Override
  public double apply(double ele1, int ele2) {
    if (ele1 == 0.0 && ele2 != 0.0)
      return initRank + ele2 * (1 - resetProb);
    else if (ele2 != 0.0)
      return ele1 + ele2 * (1 - resetProb);
    else
      return ele1;
  }

  @Override
  public float apply(float ele1, float ele2) {
    if (ele1 == 0.0 && ele2 != 0.0)
      return initRank + ele2 * (1 - resetProb);
    else if (ele2 != 0.0)
      return ele1 + ele2 * (1 - resetProb);
    else
      return ele1;
  }

  @Override
  public float apply(float ele1, long ele2) {
    if (ele1 == 0.0 && ele2 != 0.0)
      return initRank + ele2 * (1 - resetProb);
    else if (ele2 != 0.0)
      return ele1 + ele2 * (1 - resetProb);
    else
      return ele1;
  }

  @Override
  public float apply(float ele1, int ele2) {
    if (ele1 == 0.0 && ele2 != 0.0)
      return initRank + ele2 * (1 - resetProb);
    else if (ele2 != 0.0)
      return ele1 + ele2 * (1 - resetProb);
    else
      return ele1;
  }

  @Override
  public long apply(long ele1, long ele2) {
    return 0;
  }

  @Override
  public long apply(long ele1, int ele2) {
    return 0;
  }

  @Override
  public int apply(int ele1, int ele2) {
    return 0;
  }
}
