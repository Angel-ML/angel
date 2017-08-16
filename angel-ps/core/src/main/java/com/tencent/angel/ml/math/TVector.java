/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.math;


/**
 * The vector oriented column, represented a one-dimensional values.
 */
public abstract class TVector extends TAbstractVector {

  /**
   * Instantiates a new T vector.
   */
  public TVector() {
    super();
  }

  /**
   * Instantiates a new T vector.
   *
   * @param other the other
   */
  public TVector(TVector other) {
    super(other);
  }

  /**
   *Plus this vector with other vector element by element.
   *
   * @param other the other vector
   * @return the reference of object
   */
  public abstract TVector plusBy(TAbstractVector other);

  /**
   * Plus a element of vector.
   *
   * @param index the element index
   * @param x     the double update value
   * @return the reference of object
   */
  public abstract TVector plusBy(int index, double x);

  /**
   * Multiply plus this vector with other vector element by element.
   *
   * @param other the other
   * @param x     the double multiply factor
   * @return the reference of object
   */
  public abstract TVector plusBy(TAbstractVector other, double x);


  /**
   * Plus this vector with other vector to generate a new vector element by element.
   *
   * @param other the other vector
   * @return the new vector
   */
  public abstract TVector plus(TAbstractVector other);

  /**
   * Multiply plus this vector with other vector to a generate new vector element by element.
   *
   * @param other the other
   * @param x     the double multiply factor
   * @return the new vector
   */
  public abstract TVector plus(TAbstractVector other, double x);

  /**
   * Dot this vector with other.
   *
   * @param other the other
   * @return the result
   */
  public abstract double dot(TAbstractVector other);

  /**
   * Times this vector with x, generating a new vector
   *
   * @param x the double multiply factor
   * @return the new vector
   */
  public abstract TVector times(double x);

  /**
   * Times this vector with x
   *
   * @param x the double multiply factor
   * @return the reference of object
   */
  public abstract TVector timesBy(double x);

  /**
   * Filter value whose absolute value less than x.
   *
   * @param x the comparison value
   * @return the t vector
   */
  public abstract TVector filter(double x);

  /**
   * Clear.
   */
  public abstract void clear();

  /**
   * Non zero number long.
   *
   * @return the long
   */
  public abstract long nonZeroNumber();


  /**
   * Calculate square norm value of vector
   * @return square norm value
   */
  public abstract double squaredNorm();

  /**
   * Clone.
   *
   * @param vector for cloning the vector
   */
  public void clone(TVector vector) {
    assert this.getClass() == vector.getClass();
    assert this.dim == vector.dim;
    this.matrixId = vector.matrixId;
    this.rowId = vector.rowId;
    this.clock = vector.clock;
    this.dim = vector.dim;
  }

  /**
   * Clone a vector.
   *
   * @return  cloned vector
   */
  public abstract TVector clone();
}
