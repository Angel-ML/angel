package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TVector;

/**
 * Base class of double vector
 */
public abstract class TDoubleVector extends TVector {

  public TDoubleVector() {
    super();
  }

  public TDoubleVector(TDoubleVector other) {
    super(other);
  }

  /**
   * Get all indexes of vector
   * @return all indexes of vector
   */
  public abstract int[] getIndices();

  /**
   * Get all values of vector
   * @return all values of vector
   */
  public abstract double [] getValues();

  /**
   * Get a element value
   * @param index element index
   * @return element value
   */
  public abstract double get(int index);

  /**
   * Set a element value
   * @param index element index
   * @param value element value
   */
  public abstract void set(int index, double value);

  /**
   * Get square norm value
   * @return square norm value
   */
  public abstract double squaredNorm();

  /**
   * Clone vector
   * @return cloned vector
   */
  public abstract TDoubleVector clone();

  /**
   * Plus a element by a update value
   * @param index element value
   * @param delta update value
   * @return this
   */
  public abstract TDoubleVector plusBy(int index, double delta);

}
