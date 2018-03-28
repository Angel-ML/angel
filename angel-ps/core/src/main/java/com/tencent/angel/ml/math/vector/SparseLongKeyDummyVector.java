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

/**
 * Sparse double vector with long key, it only contains the element indexes as the values are always 1.
 */
public class SparseLongKeyDummyVector extends TVector {
  /**
   * The size of alloc more
   */
  private final static int ALLOC_MORE_SIZE = 256;
  private long longDim;

  /**
   * The init size of index array
   */
  private final static int INIT_ALLOC_SIZE = 128;

  /**
   * The index array
   */
  long[] indices = null;

  /**
   * The capacity of the vector
   */
  int capacity = -1;

  /**
   * Nonzeor element number
   */
  int nonzero = -1;

  /**
   * Init the empty vector
   */
  public SparseLongKeyDummyVector() {
    this(-1);
  }

  /**
   * Init the vector with the vector dimension
   *
   * @param dim vector dimension
   */
  public SparseLongKeyDummyVector(long dim) {
    this(dim, INIT_ALLOC_SIZE);
  }

  /**
   * Init the vector with the vector dimension and index array capacity
   *
   * @param dim vector dimension
   * @param capacity index array capacity
   */
  public SparseLongKeyDummyVector(long dim, int capacity) {
    super();
    this.longDim = dim;
    this.capacity = capacity;
    this.nonzero = 0;
    this.indices = new long[capacity];
  }

  public SparseLongKeyDummyVector(long[] indices, long dim) {
    super();
    this.longDim = dim;
    this.capacity = indices.length;
    this.nonzero = indices.length;
    this.indices = indices;
  }

  public long getLongDim() { return longDim; }

  private void allocMore() {
    int allocSize = capacity + ALLOC_MORE_SIZE;
    long[] allocIndexes = new long[allocSize];
    System.arraycopy(indices, 0, allocIndexes, 0, nonzero);
    capacity = allocSize;
    indices = allocIndexes;
  }

  @Override public SparseLongKeyDummyVector clone() {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  public double sum() {
    return nonzero * 1.0;
  }

  public long[] getIndices() {
    return indices;
  }

  public int getNonzero() {
    return nonzero;
  }

  @Override public RowType getType() {
    return RowType.T_DOUBLE_SPARSE_LONGKEY;
  }

  @Override public int size() {
    return nonzero;
  }

  @Override public double sparsity() {
    return ((double) nonzero) / dim;
  }

  public TVector plusBy(long index, double x) { throw new UnsupportedOperationException("Unsupport operation"); }

  public void set(long index, double value) {
    if (nonzero >= indices.length) {
      allocMore();
    }
    indices[nonzero++] = index;
  }

  public double get(long index) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  public long[] getIndexes() {
    return indices;
  }

  public double[] getValues() {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TVector plusBy(TAbstractVector other) { throw new UnsupportedOperationException("Unsupport operation"); }

  @Override public TVector plusBy(int index, double x) { throw new UnsupportedOperationException("Unsupport operation"); }

  @Override public TVector plusBy(TAbstractVector other, double x) { throw new UnsupportedOperationException("Unsupport operation"); }

  @Override public TVector plus(TAbstractVector other) { throw new UnsupportedOperationException("Unsupport operation"); }

  @Override public TVector plus(TAbstractVector other, double x) { throw new UnsupportedOperationException("Unsupport operation"); }

  @Override public double dot(TAbstractVector other) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TVector times(double x) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TVector timesBy(double x) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public TVector filter(double x) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public void clear() {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public long nonZeroNumber() {
    return nonzero;
  }

  @Override public double squaredNorm() {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  public double norm() {
    throw new UnsupportedOperationException("Unsupport operation");
  }
}
