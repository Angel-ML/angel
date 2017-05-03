package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TVector;

/**
 * Created by leleyu on 2015/12/25.
 */
public abstract class TDoubleVector extends TVector {

  public TDoubleVector() {
    super();
  }

  public TDoubleVector(TDoubleVector other) {
    super(other);
  }

  public abstract double [] getValues();

  public abstract double get(int index);

  public abstract void set(int index, double value);

  public abstract int[] getIndices();

  public abstract double squaredNorm();

  public abstract TDoubleVector clone();

}
