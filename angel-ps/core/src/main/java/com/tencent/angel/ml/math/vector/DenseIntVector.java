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
 */

package com.tencent.angel.ml.math.vector;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.vector.TIntVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.VectorType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DenseIntVector extends TIntVector {

  private final static Log LOG = LogFactory.getLog(DenseIntVector.class);

  /**
   * the value of vector
   */
  int[] values;

  /**
   * init the empty vector
   */
  public DenseIntVector() {
    super();
  }

  /**
   * init the vector by another vector
   *
   * @param other
   */
  public DenseIntVector(DenseIntVector other) {
    super(other);
    values = new int[dim];
    System.arraycopy(other.values, 0, values, 0, dim);
  }

  /**
   * init the vector by setting the dim
   *
   * @param dim
   */
  public DenseIntVector(int dim) {
    super();
    this.dim = dim;
    values = new int[dim];
  }

  /**
   * init the vector by setting the dim and values
   *
   * @param dim
   * @param values
   */
  public DenseIntVector(int dim, int[] values) {
    super();
    this.dim = dim;
    this.values = values;
  }

  /**
   * add the value to one element
   *
   * @param index
   * @param x
   */
  public void add(int index, int x) {
    this.values[index] += x;
  }

  /**
   * clone the vector
   *
   * @return
   */
  @Override
  public TVector clone() {
    return new DenseIntVector(this);
  }

  /**
   * clone vector by another one
   *
   * @return
   */
  @Override
  public void clone(TVector row) {
    DenseIntVector denseIntRow = (DenseIntVector) row;
    System.arraycopy(denseIntRow.values, 0, this.values, 0, dim);
  }

  /**
   * clear the vector
   */
  @Override
  public void clear() {
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        values[i] = 0;
      }
    }
  }

  /**
   * calculate the inner product
   *
   * @param other
   * @return
   */
  @Override
  public double dot(TAbstractVector other) {
    return 0;
  }

  /**
   * filter the vector
   *
   * @param x the comparison value
   * @return
   */
  @Override
  public TIntVector filter(double x) {
    return this;
  }

  /**
   * get the element by index
   *
   * @param index the index
   * @return
   */
  @Override
  public int get(int index) {
    return values[index];
  }

  /**
   * get values of all of the elements
   *
   * @return
   */
  @Override
  public int[] getValues() {
    return values;
  }

  /**
   * get index of all of the elements
   *
   * @return
   */
  @Override
  public int[] getIndices() {
    return null;
  }

  /**
   * get the type
   *
   * @return
   */
  @Override
  public VectorType getType() {
    return VectorType.T_INT_DENSE;
  }

  @Override
  public void inc(int index, int value) {
    values[index] += value;
  }

  /**
   * count the nonzero element
   *
   * @return
   */
  @Override
  public long nonZeroNumber() {
    long ret = 0;
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        if (values[i] != 0) {
          ret++;
        }
      }
    }

    return ret;
  }

  /**
   * plus the vector by another vector
   *
   * @param other the other
   * @return
   */
  @Override
  public TVector plusBy(TAbstractVector other) {
    if (other instanceof DenseIntVector) {
      return plusBy((DenseIntVector) other);
    }

    LOG.error(String.format("Error Vector type %s", other.getClass().getName()));
    return null;
  }

  /**
   * plus the vector by another vector
   *
   * @param other the other
   * @return
   */
  public TIntVector plusBy(DenseIntVector other) {
    for (int i = 0; i < dim; i++)
      values[i] += other.values[i];
    return this;
  }

  @Override
  public TVector plusBy(TAbstractVector other, double x) {

    return null;
  }

  /**
   * plus the vector by another vector and element
   *
   * @param other the other
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TIntVector plusBy(TAbstractVector other, int x) {
    if (other instanceof DenseIntVector)
      return plusBy((DenseIntVector) other, x);

    LOG.error(String.format("Error Vector type %s", other.getClass().getName()));
    return null;
  }

  /**
   * plus the vector by another DenseIntVector and element
   *
   * @param other the other
   * @param x the double multiply factor
   * @return
   */
  public TIntVector plusBy(DenseIntVector other, int x) {
    for (int i = 0; i < dim; i++)
      values[i] += other.values[i] * x;
    return this;
  }

  @Override
  public TVector plus(TAbstractVector other) {
    return null;
  }

  @Override
  public TVector plus(TAbstractVector other, double x) {
    return null;
  }

  @Override
  public TVector plus(TAbstractVector other, int x) {
    return null;
  }

  /**
   * set the value by index
   *
   * @param index the index
   * @param value the value
   */
  @Override
  public void set(int index, int value) {
    values[index] = value;
  }

  /**
   * get the sparsity
   *
   * @return
   */
  @Override
  public double sparsity() {
    int sum = 0;
    for (int i = 0; i < values.length; i++)
      if (values[i] != 0)
        sum++;

    return ((double) sum) / values.length;
  }

  /**
   * get the size
   *
   * @return
   */
  @Override
  public int size() {
    return values.length;
  }

  @Override
  public TVector times(double x) {
    return null;
  }

  @Override
  public TVector timesBy(double x) {
    return null;
  }

}
