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

  public abstract double sum();
}
