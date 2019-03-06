package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

public class DivNonZero extends Binary {

  public DivNonZero(boolean inplace) {
    setInplace(inplace);
    setKeepStorage(Constant.keepStorage);
  }

  @Override
  public OpType getOpType() {
    return OpType.UNION;
  }

  @Override
  public double apply(double ele1, double ele2) {
    if (ele2 == 0) {
      return ele1;
    } else {
      return ele1 / ele2;
    }
  }

  @Override
  public double apply(double ele1, float ele2) {
    if (ele2 == 0) {
      return ele1;
    } else {
      return ele1 / ele2;
    }
  }

  @Override
  public double apply(double ele1, long ele2) {
    if (ele2 == 0) {
      return ele1;
    } else {
      return ele1 / ele2;
    }
  }

  @Override
  public double apply(double ele1, int ele2) {
    if (ele2 == 0) {
      return ele1;
    } else {
      return ele1 / ele2;
    }
  }

  @Override
  public float apply(float ele1, float ele2) {
    if (ele2 == 0) {
      return ele1;
    } else {
      return ele1 / ele2;
    }
  }

  @Override
  public float apply(float ele1, long ele2) {
    if (ele2 == 0) {
      return ele1;
    } else {
      return ele1 / ele2;
    }
  }

  @Override
  public float apply(float ele1, int ele2) {
    if (ele2 == 0) {
      return ele1;
    } else {
      return ele1 / ele2;
    }
  }

  @Override
  public long apply(long ele1, long ele2) {
    if (ele2 == 0) {
      return ele1;
    } else {
      return ele1 / ele2;
    }
  }

  @Override
  public long apply(long ele1, int ele2) {
    if (ele2 == 0) {
      return ele1;
    } else {
      return ele1 / ele2;
    }
  }

  @Override
  public int apply(int ele1, int ele2) {
    if (ele2 == 0) {
      return ele1;
    } else {
      return ele1 / ele2;
    }
  }
}
