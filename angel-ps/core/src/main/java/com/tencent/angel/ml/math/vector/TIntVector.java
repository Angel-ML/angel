package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TVector;

public abstract class TIntVector extends TVector {
  public TIntVector() {
    super();
  }

  public TIntVector(TIntVector other) {
    super(other);
  }

  public abstract int get(int index);

  public abstract void set(int index, int value);

  public abstract void inc(int index, int value);

  public abstract int[] getValues();

  public abstract int[] getIndices();

  public abstract TIntVector filter(double x);
}
