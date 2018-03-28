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
import com.tencent.angel.ml.matrix.RowType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

/**
 * Sparse Double Vector using one array as its backend storage. The vector indexes are sorted in ascending order.
 */
public class SparseDoubleSortedVector extends TIntDoubleVector {

  private final static Log LOG = LogFactory.getLog(SparseDoubleSortedVector.class);

  /**
   * Sorted index for non-zero items
   */
  int[] indices;

  /**
   * Number of non-zero items in this vector
   */
  int nnz;

  /**
   * Non-zero element values
   */
  public double[] values;

  /**
   * init the empty vector
   */
  public SparseDoubleSortedVector() {
    super();
  }

  /**
   * Init the vector with the vector dimension and index array capacity
   *
   * @param dim      vector dimension
   * @param capacity index array capacity
   */
  public SparseDoubleSortedVector(int capacity, int dim) {
    super();
    this.nnz = 0;
    this.dim = dim;
    this.indices = new int[capacity];
    this.values = new double[capacity];
  }

  /**
   * Init the vector with the vector dimension, sorted non-zero indexes and values
   *
   * @param dim     vector dimension
   * @param indices sorted non-zero indexes
   * @param values  non-zero values
   */
  public SparseDoubleSortedVector(int dim, int[] indices, double[] values) {
    super();
    this.nnz = indices.length;
    this.dim = dim;
    this.indices = indices;
    this.values = values;
  }

  /**
   * Init the vector by another vector
   *
   * @param other a SparseDoubleSortedVector with same dimension with this vector
   */
  public SparseDoubleSortedVector(SparseDoubleSortedVector other) {
    super(other);
    this.nnz = other.nnz;
    this.indices = new int[nnz];
    this.values = new double[nnz];
    System.arraycopy(other.indices, 0, this.indices, 0, this.nnz);
    System.arraycopy(other.values, 0, this.values, 0, nnz);
  }

  @Override public TIntDoubleVector plusBy(int index, double delta) {
    set(index, get(index) + delta);
    return this;
  }

  @Override public double sum() {
    double ret = 0.0;
    for (int i = 0; i < values.length; i++) {
      ret += values[i];
    }
    return ret;
  }

  @Override
  public TIntDoubleVector elemUpdate(IntDoubleElemUpdater updater, ElemUpdateParam param) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public SparseDoubleSortedVector clone() {
    return new SparseDoubleSortedVector(this);
  }

  @Override public void clone(TVector row) {
    SparseDoubleSortedVector sortedRow = (SparseDoubleSortedVector) row;
    if (nnz == sortedRow.nnz) {
      System.arraycopy(sortedRow.indices, 0, this.indices, 0, this.nnz);
      System.arraycopy(sortedRow.values, 0, this.values, 0, nnz);
    } else {
      this.nnz = sortedRow.nnz;
      this.indices = new int[nnz];
      this.values = new double[nnz];
      System.arraycopy(sortedRow.indices, 0, this.indices, 0, this.nnz);
      System.arraycopy(sortedRow.values, 0, this.values, 0, nnz);
    }
  }

  @Override public void clear() {
    this.nnz = 0;
    if (this.indices != null)
      this.indices = null;
    if (this.values != null)
      this.values = null;
  }

  @Override public double dot(TAbstractVector other) {
    if (other instanceof TIntDoubleVector)
      return dot((TIntDoubleVector) other);
    else if (other instanceof TLongDoubleVector)
      return dot((TLongDoubleVector) other);
    else if (other instanceof TIntFloatVector)
      return dot((TIntFloatVector) other);
    else if (other instanceof TLongFloatVector)
      return dot((TLongFloatVector) other);
    else throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " dot " + other.getClass()
        .getName());
  }

  private double dot(TIntDoubleVector other) {
    double ret = 0.0;
    int[] indexs = this.indices;
    double[] values = this.values;
    for (int i = 0; i < this.nnz; i++) {
      ret += values[i] * other.get(indexs[i]);
    }
    return ret;
  }

  private double dot(TLongDoubleVector other) {
    double ret = 0.0;
    int[] indexs = this.indices;
    double[] values = this.values;
    for (int i = 0; i < this.nnz; i++) {
      ret += values[i] * other.get(indexs[i]);
    }
    return ret;
  }

  private double dot(TIntFloatVector other) {
    double ret = 0.0;
    int[] indexs = this.indices;
    double[] values = this.values;
    for (int i = 0; i < this.nnz; i++) {
      ret += values[i] * other.get(indexs[i]);
    }
    return ret;
  }

  private double dot(TLongFloatVector other) {
    double ret = 0.0;
    int[] indexs = this.indices;
    double[] values = this.values;
    for (int i = 0; i < this.nnz; i++) {
      ret += values[i] * other.get(indexs[i]);
    }
    return ret;
  }

  @Override public TIntDoubleVector filter(double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override public double get(int index) {
    int position = Arrays.binarySearch(indices, 0, nnz, index);
    if (position >= 0) {
      return values[position];
    }

    return 0.0;
  }

  @Override public int[] getIndices() {
    return indices;
  }

  @Override public RowType getType() {
    return RowType.T_DOUBLE_SPARSE;
  }

  @Override public double[] getValues() {
    return values;
  }

  @Override public long nonZeroNumber() {
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

  @Override public TIntDoubleVector plus(TAbstractVector other) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override public TIntDoubleVector plus(TAbstractVector other, double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override public TIntDoubleVector plusBy(TAbstractVector other) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override public TIntDoubleVector plusBy(TAbstractVector other, double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override public void set(int index, double value) {
    this.indices[nnz] = index;
    this.values[nnz] = value;
    nnz++;
  }

  @Override public int size() {
    return nnz;
  }

  @Override public double sparsity() {
    return ((double) nnz) / dim;
  }

  @Override public double squaredNorm() {
    if (values == null) {
      return 0.0;
    }

    double norm = 0.0;
    for (int i = 0; i < values.length; i++)
      norm += values[i] * values[i];
    return norm;
  }

  @Override public double norm() {
    if (values == null) {
      return 0.0;
    }

    double norm = 0.0;
    for (int i = 0; i < values.length; i++)
      norm += Math.abs(values[i]);
    return norm;
  }

  @Override public TIntDoubleVector times(double x) {
    SparseDoubleSortedVector vector = this.clone();
    for (int i = 0; i < vector.nnz; i++)
      vector.values[i] *= x;
    return vector;
  }

  @Override public TIntDoubleVector timesBy(double x) {
    for (int i = 0; i < nnz; i++)
      values[i] *= x;
    return this;
  }
}
