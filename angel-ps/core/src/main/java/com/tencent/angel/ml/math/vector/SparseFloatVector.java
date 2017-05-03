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
import com.tencent.angel.ml.math.vector.TFloatVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.VectorType;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
   * init the vector by setting the dim
   *
   * @param dim
   */
  public SparseFloatVector(int dim) {
    this(dim, INIT_SIZE);
  }

  /**
   * init the vector by setting the dim and capacity
   *
   * @param dim
   * @param capacity
   */
  public SparseFloatVector(int dim, int capacity) {
    super();
    this.hashMap = new Int2FloatOpenHashMap(capacity);
    this.dim = dim;
  }

  /**
   * init the vector by setting the dim , index and value
   *
   * @param dim
   * @param indices
   * @param values
   */
  public SparseFloatVector(int dim, int[] indices, float[] values) {
    super();
    this.dim = dim;
    this.hashMap = new Int2FloatOpenHashMap(indices, values);
  }

  /**
   * init the vector by setting the dim map
   * 
   * @param dim
   * @param map
   */
  public SparseFloatVector(int dim, Int2FloatOpenHashMap map) {
    super();
    this.dim = dim;
    this.hashMap = map;
  }

  /**
   * init the vector by another vector
   * 
   * @param other
   */
  public SparseFloatVector(SparseFloatVector other) {
    super(other);
    this.hashMap = new Int2FloatOpenHashMap(other.hashMap);
  }

  /**
   * clone the vector
   *
   * @return
   */
  @Override
  public TFloatVector clone() {
    return new SparseFloatVector(this);
  }

  /**
   * clone vector by another one
   *
   * @return
   */
  @Override
  public void clone(TVector row) {
    SparseFloatVector sparseFloatVector = (SparseFloatVector) row;
    hashMap.clear();
    hashMap.putAll(sparseFloatVector.hashMap);
  }

  /**
   * clear the vector
   */
  @Override
  public void clear() {
    if (hashMap != null)
      hashMap.clear();;
  }

  /**
   * get the inner product of two vector
   * 
   * @param other the other
   * @return
   */
  @Override
  public double dot(TAbstractVector other) {
    if (other instanceof DenseFloatVector)
      return dot((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return dot((SparseFloatVector) other);
    return 0;
  }

  public double dot(DenseFloatVector other) {
    double dot = 0.0;

    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();

    Int2FloatMap.Entry entry = null;

    while (iter.hasNext()) {
      entry = iter.next();
      dot += entry.getFloatValue() * other.values[entry.getIntKey()];
    }
    return dot;
  }

  public double dot(SparseFloatVector other) {
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


  /**
   * filter the vector and covert to the appropriate type
   *
   * @param x the comparison value
   * @return
   */
  @Override
  public TVector filter(double x) {
    Int2FloatOpenHashMap newMap = new Int2FloatOpenHashMap();

    ObjectIterator<Int2FloatMap.Entry> iter = hashMap.int2FloatEntrySet().fastIterator();

    Int2FloatMap.Entry entry = null;

    while (iter.hasNext()) {
      entry = iter.next();
      float value = entry.getFloatValue();
      if (Math.abs(value) > x) {
        newMap.put(entry.getIntKey(), value);
      }
    }
    SparseFloatVector vector = new SparseFloatVector(dim, newMap);

    return null;
  }

  /**
   * get the element by index
   *
   * @param index the index
   * @return
   */
  @Override
  public float get(int index) {
    return this.hashMap.get(index);
  }

  /**
   * get index of all of the elements
   *
   * @return
   */
  @Override
  public int[] getIndices() {
    return hashMap.keySet().toIntArray();
  }

  /**
   * get typr of vector
   *
   * @return
   */
  @Override
  public VectorType getType() {
    return VectorType.T_FLOAT_SPARSE;
  }

  /**
   * get values of vector
   *
   * @return
   */
  @Override
  public float[] getValues() {
    return hashMap.values().toFloatArray();
  }

  /**
   * count the nonzero element
   *
   * @return
   */
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
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other);

    return null;
  }

  public TVector plus(DenseFloatVector other) {
    DenseFloatVector vec = new DenseFloatVector(other);

    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();

    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      float oldvalue = vec.get(entry.getIntKey());
      vec.set(entry.getIntKey(), oldvalue + entry.getFloatValue());
    }

    return vec;
  }

  public TVector plus(SparseFloatVector other) {
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
  public TVector plus(TAbstractVector other, double x) {
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other, x);
    if (other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other, x);

    return null;
  }

  @Override
  public TVector plus(TAbstractVector other, int x) {
    return plus(other, (double) x);
  }

  public TVector plus(TAbstractVector other, float x) {
    return plus(other, (double) x);
  }

  public TVector plus(DenseFloatVector other, double x) {
    DenseFloatVector vec = new DenseFloatVector(dim, other.getValues());
    vec.timesBy(x);
    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();

    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      LOG.info("iterate.");
      entry = iter.next();
      double delt = vec.get(entry.getIntKey());
      vec.set(entry.getIntKey(), entry.getFloatValue() + delt);
    }

    return vec;
  }

  public TVector plus(SparseFloatVector other, double x) {
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
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other);

    return null;
  }

  public TVector plusBy(DenseFloatVector other) {
    float[] delt = other.getValues();
    for (int i = 0; i < delt.length; i++)
      hashMap.addTo(i, delt[i]);

    return this;
  }

  public TVector plusBy(SparseFloatVector other) {
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.addTo(entry.getIntKey(), entry.getFloatValue());
    }
    return this;
  }

  @Override
  public TVector plusBy(TAbstractVector other, double x) {
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other, x);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other, x);
    return null;
  }

  @Override
  public TVector plusBy(TAbstractVector other, int x) {
    return plusBy(other, (double) x);
  }

  public TVector plusBy(TAbstractVector other, float x) {
    return plusBy(other, (double) x);
  }

  public TVector plusBy(DenseFloatVector other, double x) {
    float[] delt = other.getValues();
    float fx = (float) x;
    for (int i = 0; i < delt.length; i++)
      hashMap.addTo(i, fx * delt[i]);

    return this;
  }

  public TVector plusBy(SparseFloatVector other, double x) {
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    float fx = (float) x;
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.addTo(entry.getIntKey(), fx * entry.getFloatValue());
    }

    return this;
  }

  /**
   * set the double value by index
   *
   * @param index the index
   * @param value the value
   */
  @Override
  public void set(int index, double value) {
    this.hashMap.put(index, (float) value);
  }

  /**
   * set the float value by index
   *
   * @param index the index
   * @param value the value
   */
  public void set(int index, float value) {
    this.hashMap.put(index, value);
  }

  /**
   * get the norm of vector
   *
   * @return
   */
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

  /**
   * get the sparsity
   *
   * @return
   */
  @Override
  public double sparsity() {
    return ((double) hashMap.size() / dim);
  }

  /**
   * get the size
   *
   * @return
   */
  @Override
  public int size() {
    return hashMap.size();
  }

  /**
   * the multiplication of vector and element and do not change the vector
   *
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TVector times(double x) {
    SparseFloatVector vector = new SparseFloatVector(this.dim);

    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();

    Int2FloatMap.Entry entry = null;
    float fx = (float) x;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.hashMap.put(entry.getIntKey(), fx * entry.getFloatValue());
    }

    return vector;
  }

  /**
   * the multiplication of vector and element and change the vector
   *
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TVector timesBy(double x) {
    ObjectIterator<Int2FloatMap.Entry> iter = this.hashMap.int2FloatEntrySet().fastIterator();

    Int2FloatMap.Entry entry = null;
    float fx = (float) x;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.put(entry.getIntKey(), fx * entry.getFloatValue());
    }

    return this;
  }
}
