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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.TVector;

public abstract class TIntVector extends TVector {
  public TIntVector() {
    super();
  }

  public TIntVector(TIntVector other) {
    super(other);
  }

  /**
   * Get element value
   * @param index element index
   * @return element value
   */
  public abstract int get(int index);

  /**
   * Set element value
   * @param index element index
   * @param value element value
   * @return this
   */
  public abstract TIntVector set(int index, int value);

  /**
   * Get all indexes of vector
   * @return all indexes of vector
   */
  public abstract int[] getIndices();

  /**
   * Get all element values
   * @return all element values
   */
  public abstract int[] getValues();

  /**
   * Plus a element with a update value
   * @param index element index
   * @param delta update value
   * @return this
   */
  public abstract TIntVector plusBy(int index, int delta);

  /**
   * Filter elements that absolute value less than a specific value if need.
   * @param x a int value
   * @return If over half part elements are filtered, return a new sparse int vector, otherwise just return this
   */
  public abstract TIntVector filter(int x);

  /**
   * Times all elements by a int factor
   * @param x factor
   * @return a new vector
   */
  public abstract TIntVector times(int x);

  /**
   * Times all elements by a int factor
   * @param x factor
   * @return this
   */
  public abstract TIntVector timesBy(int x);

  /**
   * Plus the vector with a update vector that has same dimension
   * @param other update vector
   * @param x factor
   * @return a new result vector
   */
  public abstract TVector plus(TAbstractVector other, int x);

  /**
   * Plus the vector with a update vector that has same dimension
   * @param other update vector
   * @param x factor
   * @return this
   */
  public abstract TIntVector plusBy(TAbstractVector other, int x);

  public abstract TIntVector clone();


  @Override
  public TVector plusBy(int index, double delta) { return  plusBy(index, (int) delta);}

  @Override
  public TVector filter(double x) { return  filter((int)x); }

  @Override
  public TVector times(double x) { return times((int)x); }

  @Override
  public TVector timesBy(double x) { return  timesBy((int) x); }

  @Override
  public TVector plus(TAbstractVector other, double x) { return  plus(other, (int) x); }

  @Override
  public TVector plusBy(TAbstractVector other, double x) { return  plusBy(other, (int) x); }

  public abstract long sum();
}
