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
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sparse float vector, it use a (int, float) map to store elements.
 */
public class SparseFloatVector extends TFloatVector {
  private final static Log LOG = LogFactory.getLog(SparseFloatVector.class);

  /**
   * the init size
   */
  private final static int INIT_SIZE = 64;

  /**
   * store the value
   */
  final Int2FloatOpenHashMap hashMap;

  /**
   * init the empty vector
   */
  public SparseFloatVector() {
    super();
    this.hashMap = new Int2FloatOpenHashMap(INIT_SIZE);
  }

  /**
   * init the vector by setting the dimension
   *
   * @param dim vector dimension
   */
  public SparseFloatVector(int dim) {
    this(dim, INIT_SIZE);
  }

  /**
   * init the vector by setting the dim and capacity
   *
   * @param dim vector dimension
   * @param capacity map capacity
   */
  public SparseFloatVector(int dim, int capacity) {
    super();
    this.hashMap = new Int2FloatOpenHashMap(capacity);
    this.dim = dim;
  }

  /**
   * init the vector by setting the dim , index and value
   *
   * @param dim vector dimension
   * @param indices index array
   * @param values value array which same dimension with index array
   */
  public SparseFloatVector(int dim, int[] indices, float[] values) {
    super();
    assert indices.length == values.length;
    this.dim = dim;
    this.hashMap = new Int2FloatOpenHashMap(indices, values);
  }

  /**
   * init the vector by setting the dim map
   * 
   * @param dim vector dimension
   * @param map a (int, float) map
   */
  public SparseFloatVector(int dim, Int2FloatOpenHashMap map) {
    super();
    this.dim = dim;
    this.hashMap = map;
  }

  /**
   * init the vector by another vector
   * 
   * @param other other vector which has same dimension
   */
  public SparseFloatVector(SparseFloatVector other) {
    super(other);
    this.hashMap = new Int2FloatOpenHashMap(other.hashMap);
  }

  @Override
  public TFloatVector clone() {
    return new SparseFloatVector(this);
  }

  @Override public TFloatVector plusBy(int index, float delta) {
    hashMap.addTo(index, delta);
    return this;
  }

  @Override
  public void clone(TVector row) {
    super.clone(row);
    hashMap.clear();
    hashMap.putAll(((SparseFloatVector) row).hashMap);
  }

  @Override
  public void clear() {
    if (hashMap != null)
      hashMap.clear();;
  }

  @Override
  public double dot(TAbstractVector other) {
    assert dim == other.getDimension();
    if (other instanceof DenseFloatVector)
      return dot((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return dot((SparseFloatVector) other);
    if (other instanceof DenseDoubleVector)
      return dot((DenseDoubleVector) other);
    if (other instanceof SparseDoubleVector)
      return dot((SparseDoubleVector) other);
    if (other instanceof SparseDoubleSortedVector)
      return dot((SparseDoubleSortedVector) other);

    throw new UnsupportedOperationException("Unsupport operation: " + this.getClass().getName() + " dot " + other.getClass().getName());
  }

  private double dot(DenseFloatVector other) {
    double dot = 0.0;
    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      dot += entry.getFloatValue() * other.values[entry.getIntKey()];
    }
    return dot;
  }

  private double dot(SparseFloatVector other) {
    double dot = 0.0;

    Int2FloatOpenHashMap smallMap = this.hashMap;
    Int2FloatOpenHashMap largeMap = other.hashMap;

    if (smallMap.size() > largeMap.size()) {
      smallMap = other.hashMap;
      largeMap = this.hashMap;
    }

    ObjectIterator<Int2FloatMap.Entry> iter = smallMap.int2FloatEntrySet().fastIterator();

    Int2FloatMap.Entry entry = null;

    while (iter.hasNext()) {
      entry = iter.next();
      if (largeMap.containsKey(entry.getIntKey())) {
        dot += entry.getFloatValue() * largeMap.get(entry.getIntKey());
      }
    }

    return dot;
  }

  private double dot(DenseDoubleVector other) {
    double dot = 0.0;
    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      dot += entry.getFloatValue() * other.values[entry.getIntKey()];
    }
    return dot;
  }

  private double dot(SparseDoubleVector other) {
    double dot = 0.0;
    if(this.size() < other.size()) {
      ObjectIterator<Int2FloatMap.Entry> iter = hashMap.int2FloatEntrySet().fastIterator();
      Int2FloatMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        if (other.hashMap.containsKey(entry.getIntKey())) {
          dot += entry.getFloatValue() * other.hashMap.get(entry.getIntKey());
        }
      }
    } else {
      ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
      Int2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        if (this.hashMap.containsKey(entry.getIntKey())) {
          dot += entry.getDoubleValue() * this.hashMap.get(entry.getIntKey());
        }
      }
    }
    return dot;
  }

  private double dot(SparseDoubleSortedVector other) {
    double dot = 0.0;
    int [] indexes = other.getIndices();
    double [] values = other.getValues();

    for(int i = 0; i < indexes.length; i++) {
      if(hashMap.containsKey(indexes[i])) {
        dot += hashMap.get(indexes[i]) * values[i];
      }
    }
    return dot;
  }

  @Override
  public TFloatVector filter(float x) {
    SparseFloatVector  vector = new SparseFloatVector(dim);
    vector.setMatrixId(matrixId).setRowId(rowId).setClock(clock);

    ObjectIterator<Int2FloatMap.Entry> iter = hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      float value = entry.getFloatValue();
      if (Math.abs(value) > x) {
        vector.set(entry.getIntKey(), value);
      }
    }
    return vector;
  }

  @Override
  public float get(int index) {
    return this.hashMap.get(index);
  }

  @Override
  public int[] getIndices() {
    return hashMap.keySet().toIntArray();
  }

  @Override
  public VectorType getType() {
    return VectorType.T_FLOAT_SPARSE;
  }

  @Override
  public float[] getValues() {
    return hashMap.values().toFloatArray();
  }

  @Override
  public long nonZeroNumber() {
    long ret = 0;

    if (hashMap != null) {
      ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();
      while (iter.hasNext()) {
        if (iter.next().getFloatValue() != 0)
          ret++;
      }
    }
    return ret;
  }

  @Override
  public TVector plus(TAbstractVector other) {
    assert dim == other.getDimension();
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other);

    throw new UnsupportedOperationException("Unsupport operation: " + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private TVector plus(DenseFloatVector other) {
    DenseFloatVector vec = new DenseFloatVector(other);
    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();

    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vec.plusBy(entry.getIntKey(), entry.getFloatValue());
    }

    return vec;
  }

  private TVector plus(SparseFloatVector other) {
    SparseFloatVector newVector = (SparseFloatVector) this.clone();
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();

    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      newVector.hashMap.addTo(entry.getIntKey(), entry.getFloatValue());
    }

    return newVector;
  }

  /**
   * plus the vector by another vector
   *
   * @param other the other
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TVector plus(TAbstractVector other, float x) {
    assert dim == other.getDimension();
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other, x);
    if (other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other, x);

    throw new UnsupportedOperationException("Unsupport operation: " + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private TVector plus(DenseFloatVector other, float x) {
    DenseFloatVector vec = new DenseFloatVector(other);
    vec.timesBy(x);

    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vec.plusBy(entry.getIntKey(), entry.getFloatValue());
    }
    return vec;
  }

  private TVector plus(SparseFloatVector other, float x) {
    SparseFloatVector newVector = (SparseFloatVector) this.clone();
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();

    float fx = (float) x;
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      newVector.hashMap.addTo(entry.getIntKey(), fx * entry.getFloatValue());
    }

    return newVector;
  }

  @Override
  public TVector plusBy(TAbstractVector other) {
    assert dim == other.getDimension();
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other);

    throw new UnsupportedOperationException("Unsupport operation: " + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  private TVector plusBy(DenseFloatVector other) {
    float[] delta = other.getValues();
    for (int i = 0; i < delta.length; i++)
      hashMap.addTo(i, delta[i]);

    return this;
  }

  private TVector plusBy(SparseFloatVector other) {
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.addTo(entry.getIntKey(), entry.getFloatValue());
    }
    return this;
  }

  @Override
  public TFloatVector plusBy(TAbstractVector other, float x) {
    assert dim == other.getDimension();
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other, x);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other, x);

    throw new UnsupportedOperationException("Unsupport operation: " + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  public TFloatVector plusBy(DenseFloatVector other, float x) {
    float[] delta = other.getValues();
    float fx = (float) x;
    for (int i = 0; i < delta.length; i++)
      hashMap.addTo(i, fx * delta[i]);

    return this;
  }

  public TFloatVector plusBy(SparseFloatVector other, float x) {
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    float fx = (float) x;
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.addTo(entry.getIntKey(), fx * entry.getFloatValue());
    }

    return this;
  }

  @Override
  public TFloatVector set(int index, float value) {
    this.hashMap.put(index, value);
    return this;
  }

  @Override
  public double squaredNorm() {
    ObjectIterator<Int2FloatMap.Entry> iter = hashMap.int2FloatEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      float v = iter.next().getFloatValue();
      sum += v * v;
    }
    return sum;
  }

  @Override
  public double sparsity() {
    return ((double) nonZeroNumber() / dim);
  }

  @Override
  public int size() {
    return hashMap.size();
  }

  @Override
  public TFloatVector times(float x) {
    SparseFloatVector vector = new SparseFloatVector(this.dim);

    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.hashMap.put(entry.getIntKey(), x * entry.getFloatValue());
    }

    return vector;
  }

  @Override
  public TFloatVector timesBy(float x) {
    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.put(entry.getIntKey(), x * entry.getFloatValue());
    }

    return this;
  }
}
