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
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.VectorType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

/**
 * Sparse Double Vector using one array as its backend storage. The dimension of this vector must be
 * less than 2^32
 */
public class SparseDoubleSortedVector extends TDoubleVector {

  private final static Log LOG = LogFactory.getLog(SparseDoubleSortedVector.class);

  // @brief Sorted index for non-zero items
  int[] indices;

  // @brief Number of non-zero items in this vector
  int nnz;

  // @brief Array to store values.
  public double[] values;

  // @brief sum of the square of all of element
  double norm;

  /**
   * init the empty vector
   */
  public SparseDoubleSortedVector() {
    super();
  }

  /**
   * init the vector by setting the dim and capacity
   *
   * @param dim
   */
  public SparseDoubleSortedVector(int capacity, int dim) {
    super();
    this.nnz = 0;
    this.dim = dim;
    this.indices = new int[capacity];
    this.values = new double[capacity];
  }

  /**
   * init the vector by setting the dim indices and values
   *
   * @param dim
   * @param indices
   * @param values
   */
  public SparseDoubleSortedVector(int dim, int[] indices, double[] values) {
    super();
    this.nnz = indices.length;
    this.dim = dim;
    this.indices = indices;
    this.values = values;

    norm = 0.0;
    for (int i = 0; i < values.length; i++)
      norm += values[i] * values[i];
  }

  /**
   * init the vector by another vector
   *
   * @param other
   */
  public SparseDoubleSortedVector(SparseDoubleSortedVector other) {
    super(other);
    this.nnz = other.nnz;
    this.indices = new int[nnz];
    this.values = new double[nnz];
    this.norm = other.getNorm();
    System.arraycopy(other.indices, 0, this.indices, 0, this.nnz);
    System.arraycopy(other.values, 0, this.values, 0, nnz);
  }

  @Override
  public TDoubleVector add(int index, double delt) {
    set(index, get(index) + delt);
    return this;
  }

  /**
   * clone the vector
   *
   * @return
   */
  @Override
  public SparseDoubleSortedVector clone() {
    return new SparseDoubleSortedVector(this);
  }


  /**
   * clone vector by another one
   *
   * @return
   */
  @Override
  public void clone(TVector row) {
    SparseDoubleSortedVector sortedRow = (SparseDoubleSortedVector) row;
    if (nnz == sortedRow.nnz) {
      this.norm = ((SparseDoubleSortedVector) row).norm;
      System.arraycopy(sortedRow.indices, 0, this.indices, 0, this.nnz);
      System.arraycopy(sortedRow.values, 0, this.values, 0, nnz);
    } else {
      this.nnz = sortedRow.nnz;
      this.indices = new int[nnz];
      this.values = new double[nnz];
      this.norm = ((SparseDoubleSortedVector) row).norm;
      System.arraycopy(sortedRow.indices, 0, this.indices, 0, this.nnz);
      System.arraycopy(sortedRow.values, 0, this.values, 0, nnz);
    }
  }


  /**
   * clear the vector
   */
  @Override
  public void clear() {
    this.nnz = 0;
    this.norm = 0;
    if (this.indices != null)
      this.indices = null;
    if (this.values != null)
      this.values = null;
  }

  /**
   * calculate the inner product
   *
   * @param other
   * @return
   */
  @Override
  public double dot(TAbstractVector other) {
    if (other instanceof DenseDoubleVector)
      return dot((DenseDoubleVector) other);

    LOG.error("Cannot perform dot operation on SparseDoubleSortedVector");
    return 0.0;
  }

  public double dot(DenseDoubleVector other) {
    double ret = 0.0;
    int[] indexs = this.indices;
    double[] values = this.values;
    for (int i = 0; i < this.nnz; i++) {
      ret += values[i] * other.get(indexs[i]);
    }
    return ret;
  }

  @Override
  public TDoubleVector filter(double x) {
    return null;
  }

  /**
   * get the element by index
   *
   * @param index the index
   * @return
   */
  @Override
  public double get(int index) {
    int position = Arrays.binarySearch(indices, 0, nnz, index);
    if (position >= 0) {
      return values[position];
    }

    return 0.0;
  }

  /**
   * get the dimension of vector
   *
   * @return
   */
  public int getDimension() {
    return this.dim;
  }

  /**
   * get all of the index
   *
   * @return
   */
  @Override
  public int[] getIndices() {
    return indices;
  }

  /**
   * get the type
   *
   * @return
   */
  @Override
  public VectorType getType() {
    return VectorType.T_DOUBLE_SPARSE;
  }


  /**
   * get values of all of the elements
   *
   * @return
   */
  @Override
  public double[] getValues() {
    return values;
  }

  /**
   * get the norm
   * 
   * @return
   */
  public double getNorm() {
    return this.norm;
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

  @Override
  public TDoubleVector plus(TAbstractVector other) {
    LOG.error("Cannot perform plus operation on SparseDoubleSortedVector");
    return null;
  }


  @Override
  public TDoubleVector plus(TAbstractVector other, double x) {
    LOG.error("Cannot perform plus operation on SparseDoubleSortedVector");
    return null;
  }

  @Override
  public TDoubleVector plus(TAbstractVector other, int x) {
    LOG.error("Cannot perform plus operation on SparseDoubleSortedVector");
    return null;
  }

  @Override
  public TDoubleVector plusBy(TAbstractVector other) {
    LOG.error("Cannot perform plus operation on SparseDoubleSortedVector");
    return null;
  }

  @Override
  public TDoubleVector plusBy(TAbstractVector other, double x) {
    LOG.error("Cannot perform plus operation on SparseDoubleSortedVector");
    return null;
  }

  @Override
  public TDoubleVector plusBy(TAbstractVector other, int x) {
    LOG.error("Cannot perform plus operation on SparseDoubleSortedVector");
    return null;
  }

  /**
   * set the value by index
   *
   * @param index the index
   * @param value the value
   */
  @Override
  public void set(int index, double value) {
    // LOG.error("Cannot perform plus operation on SparseDoubleSortedVector");
    this.indices[nnz] = index;
    this.values[nnz] = value;
    nnz++;
  }

  /**
   * get the size
   *
   * @return
   */
  @Override
  public int size() {
    return nnz;
  }

  /**
   * get the sparsity
   *
   * @return
   */
  @Override
  public double sparsity() {
    return ((double) nnz) / dim;
  }

  /**
   * get the norm of vector
   *
   * @return
   */
  @Override
  public double squaredNorm() {
    return norm;
  }

  /**
   * get the multiplication of vector and element and do not change the vector
   *
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TDoubleVector times(double x) {
    SparseDoubleSortedVector vector = this.clone();
    for (int i = 0; i < vector.nnz; i++)
      vector.values[i] *= x;
    return vector;
  }

  /**
   * get the multiplication of vector and element and change the vector
   *
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TDoubleVector timesBy(double x) {
    for (int i = 0; i < nnz; i++)
      values[i] *= x;
    this.norm *= x * x;
    return this;
  }

  public void calNorm() {
    this.norm = 0.0;
    for (int i = 0; i < this.nnz; i++) {
      this.norm += values[i] * values[i];
    }
  }

}
