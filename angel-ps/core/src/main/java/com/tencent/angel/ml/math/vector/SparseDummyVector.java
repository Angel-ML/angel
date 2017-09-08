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
import com.tencent.angel.ml.math.VectorType;
import com.tencent.angel.protobuf.generated.MLProtos;

public class SparseDummyVector extends TAbstractVector {

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

  /**
   * clone the vector
   *
   * @return
   */
  @Override
  public SparseDummyVector clone() {
    return null;
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
  public MLProtos.RowType getType() {
    return MLProtos.RowType.T_DOUBLE_SPARSE;
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
