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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Base class for long key double vector.
 */
public abstract class TLongDoubleVector extends TDoubleVector {
  private static final Log LOG = LogFactory.getLog(SparseLongKeyDoubleVector.class);

  /** Vector dimension */
  protected long dim;
  public TLongDoubleVector(long dim) {
    super();
    this.dim = dim;
  }

  /**
   * Get dimension for long key vector
   * @return long dimension
   */
  public long getLongDim(){
    return dim;
  }

  /**
   * Plus a element by a update value
   * @param index element index
   * @param x update value
   * @return this
   */
  public abstract TVector plusBy(long index, double x);

  /**
   * Set a element by a new value
   * @param index element index
   * @param x new value
   * @return this
   */
  public abstract void set(long index, double x);

  /**
   * Get a element value by value index
   * @param index value index
   * @return
   */
  public abstract double get(long index);

  public abstract long[] getIndexes();

  public abstract double[] getValues();

  @Override
  public abstract TLongDoubleVector clone();

  @Override
  public int getDimension() {
    throw new UnsupportedOperationException("Unsupportted operation, you should use getLongDim instead");
  }

  @Override
  public TVector plusBy(int index, double x) {
    return plusBy((long) index, x);
  }

  public abstract double sum();
}
