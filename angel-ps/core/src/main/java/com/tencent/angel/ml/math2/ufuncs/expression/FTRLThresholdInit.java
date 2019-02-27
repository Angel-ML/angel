package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

import java.util.Random;

public class FTRLThresholdInit extends Binary {

  private double alpha, beta, lambda1, lambda2;
  private double mean, stdev;
  private Random random;


  public FTRLThresholdInit(boolean inplace, double alpha, double beta, double lambda1, double lambda2, double mean, double stdev) {
    setInplace(inplace);
    setKeepStorage(Constant.keepStorage);
    this.alpha = alpha;
    this.beta = beta;
    this.lambda1 = lambda1;
    this.lambda2 = lambda2;
    this.mean = mean;
    this.stdev = stdev;
    this.random = new Random(System.currentTimeMillis());
  }

  @Override
  public OpType getOpType() {
    return OpType.UNION;
  }

  @Override
  public double apply(double zVal, double nVal) {
    if (nVal == 0.0) return mean + random.nextGaussian() * stdev;
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    }
    return result;
  }

  @Override
  public double apply(double zVal, float nVal) {
    if (nVal == 0.0) return mean + random.nextGaussian() * stdev;
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    }
    return result;
  }

  public double apply(double zVal, long nVal) {
    if (nVal == 0.0) return mean + random.nextGaussian() * stdev;
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    }
    return result;
  }

  public double apply(double zVal, int nVal) {
    if (nVal == 0.0) return mean + random.nextGaussian() * stdev;
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    }
    return result;
  }

  public float apply(float zVal, float nVal) {
    if (nVal == 0.0f) return (float) (mean + random.nextGaussian() * stdev);
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    }
    return (float) result;
  }

  public float apply(float zVal, long nVal) {
    if (nVal == 0L) return (float) (mean + random.nextGaussian() * stdev);
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    }
    return (float) result;
  }

  public float apply(float zVal, int nVal) {
    if (nVal == 0) return (float) (mean + random.nextGaussian() * stdev);

    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    }
    return (float) result;
  }

  public long apply(long zVal, long nVal) {
    if (nVal == 0L) return (long) (mean + random.nextGaussian() * stdev);
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    }
    return (long) result;
  }

  public long apply(long zVal, int nVal) {
    if (nVal == 0) return (long) (mean + random.nextGaussian() * stdev);
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    }
    return (long) result;
  }

  public int apply(int zVal, int nVal) {
    if (nVal == 0) return (int) (mean + random.nextGaussian() * stdev);
    double result = 0.0;
    if (Math.abs(zVal) > lambda1) {
      result = (-1) * (1.0 / (lambda2 + (beta + Math.sqrt(nVal)) / alpha)) * (zVal
        - Math.signum(zVal) * lambda1);
    }
    return (int) result;
  }
}
