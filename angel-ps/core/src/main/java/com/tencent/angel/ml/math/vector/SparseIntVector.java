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

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.vector.TIntVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.VectorType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SparseIntVector extends TIntVector {

  private final static Log LOG = LogFactory.getLog(SparseIntVector.class);

  /**
   * the init size
   */
  private final static int INIT_SIZE = 64;

  /**
   * store the data
   */
  final Int2IntOpenHashMap hashMap;

  /**
   * init the vector by init size
   * 
   */
  public SparseIntVector() {
    super();
    this.hashMap = new Int2IntOpenHashMap(INIT_SIZE);
  }

  /**
   * init the vector by setting the dim
   * 
   * @param dim
   */
  public SparseIntVector(int dim) {
    this(dim, INIT_SIZE);
  }

  /**
   * init the vector by setting the dim and capacity
   * 
   * @param dim
   * @param capacity
   */
  public SparseIntVector(int dim, int capacity) {
    super();
    this.dim = dim;
    this.hashMap = new Int2IntOpenHashMap(capacity);
  }

  /**
   * init the vector by setting the dim ,index and value
   * 
   * @param dim
   * @param indices
   * @param values
   */
  public SparseIntVector(int dim, int[] indices, int[] values) {
    super();
    this.dim = dim;
    this.hashMap = new Int2IntOpenHashMap(indices, values);
  }

  /**
   * init the vector by setting the map
   * 
   * @param dim
   * @param map
   */
  public SparseIntVector(int dim, Int2IntOpenHashMap map) {
    super();
    this.dim = dim;
    this.hashMap = map;
  }

  /**
   * init the vector by another one
   * 
   * @param other
   */
  public SparseIntVector(SparseIntVector other) {
    super(other);
    this.hashMap = new Int2IntOpenHashMap(other.hashMap);
  }

  /**
   * get the value by index
   * 
   * @param index the index
   * @return
   */
  @Override
  public int get(int index) {
    return hashMap.get(index);
  }

  /**
   * set the value by index
   * 
   * @param index the index
   * @param value the value
   */
  @Override
  public void set(int index, int value) {
    hashMap.put(index, value);
  }

  /**
   * inc the element by index
   * 
   * @param index the index
   * @param value the increased value
   */
  @Override
  public void inc(int index, int value) {
    hashMap.put(index, hashMap.get(index) + value);
  }

  /**
   * get the index array
   * 
   * @return
   */
  @Override
  public int[] getIndices() {
    return hashMap.keySet().toIntArray();
  }

  /**
   * get the value array
   * 
   * @return
   */
  @Override
  public int[] getValues() {
    return hashMap.values().toIntArray();
  }

  public Int2IntOpenHashMap getHashMap() {
    return hashMap;
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
  public TIntVector filter(double x) {
    return this;
  }

  @Override
  public TVector clone() {
    return new SparseIntVector(this);
  }

  @Override
  public void clear() {
    if (hashMap != null) {
      hashMap.clear();
    }
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
    SparseIntVector sparseRow = (SparseIntVector) row;
    hashMap.clear();
    hashMap.putAll(sparseRow.hashMap);
  }

  @Override
  public double sparsity() {
    return 0;
  }

  @Override
  public VectorType getType() {
    return null;
  }

  /**
   * get the type
   * 
   * @return
   */
  @Override
  public int size() {
    return hashMap.size();
  }
}
