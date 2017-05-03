package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TVector;

/**
 * Created by leleyu on 2015/12/25.
 */
public abstract class TFloatVector extends TVector {

  public TFloatVector() {
    super();
  }

  public TFloatVector(TFloatVector other) {
    super(other);
  }

  public abstract float[] getValues();

  public abstract float get(int index);

  public abstract void set(int index, double value);

  public abstract int[] getIndices();

  public abstract double squaredNorm();

  public abstract TFloatVector clone();

}
