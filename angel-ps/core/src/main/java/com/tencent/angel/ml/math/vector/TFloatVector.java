package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.TVector;

/**
 * Base class of float vector.
 */
public abstract class TFloatVector extends TVector {

  public TFloatVector() {
    super();
  }

  public TFloatVector(TFloatVector other) {
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
  public abstract float[] getValues();

  /**
   * Get a vector element by a index
   * @param index element index
   * @return element value
   */
  public abstract float get(int index);

  /**
   * Set a vector element
   * @param index element index
   * @param value element value
   */
  public abstract TFloatVector set(int index, float value);

  /**
   * Plus a update value to a vector element
   * @param index element index
   * @param delta update value
   * @return this
   */
  public abstract TFloatVector plusBy(int index, float delta );

  /**
   * Filter elements that absolute value less than a specific value if need.
   * @param x a float value
   * @return If over half part elements are filtered, return a new sparse int vector, otherwise just return this
   */
  public abstract TFloatVector filter(float x);

  /**
   * Times all elements by a int factor
   * @param x factor
   * @return a new vector
   */
  public abstract TFloatVector times(float x);

  /**
   * Times all elements by a int factor
   * @param x factor
   * @return this
   */
  public abstract TFloatVector timesBy(float x);

  /**
   * Plus the vector with a update vector that has same dimension
   * @param other update vector
   * @param x factor
   * @return a new result vector
   */
  public abstract TVector plus(TAbstractVector other, float x);

  /**
   * Plus the vector with a update vector that has same dimension
   * @param other update vector
   * @param x factor
   * @return this
   */
  public abstract TFloatVector plusBy(TAbstractVector other, float x);

  @Override
  public TVector plusBy(int index, double delta) { return  plusBy(index, (float) delta);}

  @Override
  public TVector filter(double x) { return  filter((float)x); }

  @Override
  public TVector times(double x) { return  times((float)x); }

  @Override
  public TVector timesBy(double x) { return  timesBy((float) x); }

  @Override
  public TVector plus(TAbstractVector other, double x) { return  plus(other, (float) x); }

  @Override
  public TVector plusBy(TAbstractVector other, double x) { return  plusBy(other, (float) x); }

  public abstract double sum();
}
