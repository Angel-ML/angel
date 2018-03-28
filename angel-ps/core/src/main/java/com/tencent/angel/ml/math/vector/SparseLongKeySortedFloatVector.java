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
import com.tencent.angel.ml.matrix.RowType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

/**
 * Sparse Double Vector with long key which using one array as its backend storage. The vector indexes are sorted in ascending order.
 */
public class SparseLongKeySortedFloatVector extends TLongFloatVector {
  private final static Log LOG = LogFactory.getLog(SparseLongKeySortedFloatVector.class);

  /**
   * Sorted index for non-zero items
   */
  long[] indices;

  /**
   * Number of non-zero items in this vector
   */
  int nnz;

  /**
   * Non-zero element values
   */
  public float[] values;

  /**
   * init the empty vector
   */
  public SparseLongKeySortedFloatVector() {
    super(-1);
  }

  @Override
  public int[] getIndices() {
    return new int[0];
  }

  /**
   * Init the vector with the vector dimension and index array capacity
   *
   * @param dim      vector dimension
   * @param capacity index array capacity
   */
  public SparseLongKeySortedFloatVector(int capacity, long dim) {
    super(dim);
    this.nnz = 0;
    this.indices = new long[capacity];
    this.values = new float[capacity];
  }

  /**
   * Init the vector with the vector dimension, sorted non-zero indexes and values
   *
   * @param dim     vector dimension
   * @param indices sorted non-zero indexes
   * @param values  non-zero values
   */
  public SparseLongKeySortedFloatVector(long dim, long[] indices, float[] values) {
    super(dim);
    this.nnz = indices.length;
    this.indices = indices;
    this.values = values;
  }

  /**
   * Init the vector by another vector
   *
   * @param other a SparseLongKeySortedDoubleVector with same dimension with this vector
   */
  public SparseLongKeySortedFloatVector(SparseLongKeySortedFloatVector other) {
    super(other.getLongDim());
    this.nnz = other.nnz;
    this.indices = new long[nnz];
    this.values = new float[nnz];
    System.arraycopy(other.indices, 0, this.indices, 0, this.nnz);
    System.arraycopy(other.values, 0, this.values, 0, nnz);
  }

  @Override
  public TVector plusBy(long index, float delta) {
    set(index, get(index) + delta);
    return this;
  }

  public void set(long index, float value) {
    this.indices[nnz] = index;
    this.values[nnz] = value;
    nnz++;
    //return this;
  }

  @Override public float get(long index) {
    int position = Arrays.binarySearch(indices, 0, nnz, index);
    if (position >= 0) {
      return values[position];
    }

    return 0.0f;
  }

  @Override public long[] getIndexes() {
    return indices;
  }

  @Override
  public double sum() {
    double ret = 0.0;
    for(int i = 0; i < values.length; i++) {
      ret += values[i];
    }
    return ret;
  }

  @Override public TLongFloatVector elemUpdate(LongFloatElemUpdater updater, ElemUpdateParam param) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public void clone(TVector row) {
    if(row instanceof SparseLongKeySortedFloatVector) {
      SparseLongKeySortedFloatVector sortedRow = (SparseLongKeySortedFloatVector) row;
      if (nnz == sortedRow.nnz) {
        System.arraycopy(sortedRow.indices, 0, this.indices, 0, this.nnz);
        System.arraycopy(sortedRow.values, 0, this.values, 0, nnz);
      } else {
        this.nnz = sortedRow.nnz;
        this.indices = new long[nnz];
        this.values = new float[nnz];
        System.arraycopy(sortedRow.indices, 0, this.indices, 0, this.nnz);
        System.arraycopy(sortedRow.values, 0, this.values, 0, nnz);
      }
    }

    throw new UnsupportedOperationException("Unsupport operation: clone " + row.getClass().getName() + " to " + this.getClass().getName());
  }

  @Override
  public SparseLongKeySortedFloatVector clone() {
    return new SparseLongKeySortedFloatVector(this);
  }

  @Override
  public void clear() {
    this.nnz = 0;
    if (this.indices != null)
      this.indices = null;
    if (this.values != null)
      this.values = null;
  }

  @Override
  public double dot(TAbstractVector other) {
    if (other instanceof SparseLongKeyDoubleVector)
      return dot((SparseLongKeyDoubleVector) other);
    else if (other instanceof SparseLongKeyFloatVector)
      return dot((SparseLongKeyFloatVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " dot " + other.getClass().getName());
  }

  private double dot(SparseLongKeyDoubleVector other) {
    double ret = 0.0;
    long[] indexs = this.indices;
    float[] values = this.values;
    for (int i = 0; i < this.nnz; i++) {
      ret += values[i] * other.get(indexs[i]);
    }
    return ret;
  }

  private double dot(SparseLongKeyFloatVector other) {
    double ret = 0.0;
    long[] indexs = this.indices;
    float[] values = this.values;
    for (int i = 0; i < this.nnz; i++) {
      ret += values[i] * other.get(indexs[i]);
    }
    return ret;
  }

  @Override
  public TIntDoubleVector filter(double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public RowType getType() {
    return RowType.T_FLOAT_SPARSE_LONGKEY;
  }

  @Override
  public float[] getValues() {
    return values;
  }

  @Override
  public float get(int index) { return get((long) index); }

  @Override
  public TFloatVector set(int index, float value) {
    set((long) index, value);
    return this;
  }

  @Override
  public TFloatVector plusBy(int index, float delta) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TFloatVector filter(float x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TFloatVector times(float x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TFloatVector timesBy(float x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TVector plus(TAbstractVector other, float x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TFloatVector plusBy(TAbstractVector other, float x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }


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
  public TFloatVector plus(TAbstractVector other) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TFloatVector plus(TAbstractVector other, double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TFloatVector plusBy(TAbstractVector other) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TFloatVector plusBy(TAbstractVector other, double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public int size() {
    return nnz;
  }

  @Override
  public double sparsity() {
    return ((double) nnz) / dim;
  }

  @Override
  public double squaredNorm() {
    if(values == null) {
      return 0.0;
    }

    double norm = 0.0;
    for (int i = 0; i < values.length; i++)
      norm += values[i] * values[i];
    return norm;
  }

  public double norm() {
    if(values == null) {
      return 0.0;
    }

    double norm = 0.0;
    for (int i = 0; i < values.length; i++)
      norm += Math.abs(values[i]);
    return norm;
  }

  @Override
  public TLongFloatVector times(double x) {
    throw new UnsupportedOperationException("Unsupportted operation");
  }

  @Override
  public TLongFloatVector timesBy(double x) {
    for (int i = 0; i < nnz; i++)
      values[i] *= x;
    return this;
  }
}
