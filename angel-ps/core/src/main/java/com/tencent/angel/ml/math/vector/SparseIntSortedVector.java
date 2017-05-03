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

public class SparseIntSortedVector extends TIntVector {

  /**
   * key array
   */
  int[] keys;

  /**
   * value array
   */
  int[] values;

  public SparseIntSortedVector() {
    super();
  }

  public SparseIntSortedVector(int dim, int[] keys, int[] values) {
    this.dim = dim;
    this.keys = keys;
    this.values = values;
  }

  @Override
  public int get(int index) {
    return 0;
  }

  @Override
  public void set(int index, int value) {

  }

  @Override
  public void inc(int index, int value) {

  }

  @Override
  public int[] getValues() {
    return values;
  }

  @Override
  public int[] getIndices() {
    return keys;
  }

  @Override
  public TIntVector filter(double x) {
    return null;
  }

  @Override
  public TVector plusBy(TAbstractVector other) {
    return null;
  }

  @Override
  public TVector plusBy(TAbstractVector other, double x) {
    return null;
  }

  @Override
  public TVector plusBy(TAbstractVector other, int x) {
    return null;
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

  @Override
  public double dot(TAbstractVector other) {
    return 0;
  }

  @Override
  public TVector times(double x) {
    return null;
  }

  @Override
  public TVector timesBy(double x) {
    return null;
  }

  @Override
  public TVector clone() {
    return null;
  }

  @Override
  public void clear() {

  }

  @Override
  public long nonZeroNumber() {
    return 0;
  }

  /**
   * clone the vector by another one
   * 
   * @param row
   */
  @Override
  public void clone(TVector row) {
    SparseIntVector sparseIntRow = (SparseIntVector) row;
    System.arraycopy(sparseIntRow.getValues(), 0, this.values, 0, dim);
  }

  /**
   * get the sparsity
   * 
   * @return
   */
  @Override
  public double sparsity() {
    return 0;
  }

  /**
   * get the type
   * 
   * @return
   */
  @Override
  public VectorType getType() {
    return null;
  }

  /**
   * get the size
   * 
   * @return
   */
  @Override
  public int size() {
    return 0;
  }

}
