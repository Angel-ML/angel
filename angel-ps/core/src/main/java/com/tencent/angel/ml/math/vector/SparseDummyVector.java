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

/**
 * Sparse double vector, it only contains the element indexes as the values are always 1.
 */
public class SparseDummyVector extends TVector {

  /**
   * the size of alloc more
   */
  private static int ALLOC_MORE_SIZE = 256;

  /**
   * the init size
   */
  private static int INIT_ALLOC_SIZE = 128;
  /**
   * the index array
   */
  int[] indices = null;

  /**
   * the capacity of SparseDummyVector
   */
  int capacity = -1;

  int nonzero = -1;

  /**
   * init the empty vector
   */
  public SparseDummyVector() {
    super();
  }

  /**
   * init the vector by setting the dim
   *
   * @param dim
   */
  public SparseDummyVector(int dim) {
    this.dim = dim;
    this.capacity = INIT_ALLOC_SIZE;
    this.nonzero = 0;
    this.indices = new int[INIT_ALLOC_SIZE];
  }

  public SparseDummyVector(int[] indices, int dim) {
    this.dim = dim;
    this.capacity = indices.length;
    this.nonzero = indices.length;
    this.indices = indices;
  }

  /**
   * init the vector by setting the dim and capacity
   *
   * @param dim
   * @param capacity
     */
  public SparseDummyVector(int dim, int capacity) {
    this.dim = dim;
    this.capacity = capacity;
    this.nonzero = 0;
    this.indices = new int[capacity];
  }

  /**
   *alloc more space for vector when the size is out of capacity
   */
  private void allocMore() {
    int allocSize = capacity + ALLOC_MORE_SIZE;
    int[] allocIndexes = new int[allocSize];
    System.arraycopy(indices, 0, allocIndexes, 0, nonzero);
    capacity = allocSize;
    indices = allocIndexes;
  }

  @Override
  public TVector plusBy(TAbstractVector other) {
    throw new UnsupportedOperationException("unsupport operation");
  }

  @Override
  public TVector plusBy(int index, double x) {
    throw new UnsupportedOperationException("unsupport operation");
  }

  @Override
  public TVector plusBy(TAbstractVector other, double x) {
    throw new UnsupportedOperationException("unsupport operation");
  }

  @Override
  public TVector plus(TAbstractVector other) {
    throw new UnsupportedOperationException("unsupport operation");
  }

  @Override
  public TVector plus(TAbstractVector other, double x) {
    throw new UnsupportedOperationException("unsupport operation");
  }

  @Override
  public double dot(TAbstractVector other) {
    throw new UnsupportedOperationException("unsupport operation");
  }

  @Override
  public TVector times(double x) {
    throw new UnsupportedOperationException("unsupport operation");
  }

  @Override
  public TVector timesBy(double x) {
    throw new UnsupportedOperationException("unsupport operation");
  }

  @Override
  public TVector filter(double x) {
    throw new UnsupportedOperationException("unsupport operation");
  }

  @Override
  public void clear() {

  }

  @Override
  public long nonZeroNumber() {
    throw new UnsupportedOperationException("unsupport operation");
  }

  @Override
  public double squaredNorm() {
    throw new UnsupportedOperationException("unsupport operation");
  }

  /**
   * clone the vector
   *
   * @return
   */
  @Override
  public SparseDummyVector clone() {
    throw new UnsupportedOperationException("unsupport operation");
  }

  /**
   * get all of the index
   *
   * @return
   */
  public int[] getIndices() {
    return indices;
  }

  /**
   * get the count of nonzero element
   *
   * @return
   */
  public int getNonzero() {
    return nonzero;
  }

  /**
   * get the type
   *
   * @return
     */
  @Override
  public RowType getType() {
    return RowType.T_FLOAT_SPARSE;
  }

  /**
   * get the size
   *
   * @return
     */
  @Override
  public int size() {
    return nonzero;
  }

  /**
   * get the sparsity
   *
   * @return
     */
  @Override
  public double sparsity() {
    return ((double) nonzero) / dim;
  }

  /**
   * set the value by index
   *
   * @param index the index
   * @param value the value
   */
  public void set(int index, double value) {
    if (nonzero >= indices.length) {
      allocMore();
    }
    indices[nonzero++] = index;
  }
}
