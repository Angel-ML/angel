package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TVector;

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
