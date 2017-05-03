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
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.VectorType;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * SparseLong2DoubleVector using HashMap<Long, Double> as its backend storage
 */
public class SparseDoubleVector extends TDoubleVector {

  private final static Log LOG = LogFactory.getLog(SparseDoubleVector.class);

  /**
   * the init size
   */
  private final static int INIT_SIZE = 64;

  /**
   * store the value
   */
  final Int2DoubleOpenHashMap hashMap;

  /**
   * init the empty vector
   */
  public SparseDoubleVector() {
    super();
    this.hashMap = new Int2DoubleOpenHashMap(INIT_SIZE);
  }

  /**
   * init the vector by setting the dim
   *
   * @param dim
   */
  public SparseDoubleVector(int dim) {
    this(dim, INIT_SIZE);
  }

  /**
   * init the dim and capacity for vector
   *
   * @param dim
   * @param capacity
   */
  public SparseDoubleVector(int dim, int capacity) {
    super();
    this.hashMap = new Int2DoubleOpenHashMap(capacity);
    this.dim = dim;
  }

  /**
   * init the vector by setting the dim , index and value
   *
   * @param dim
   * @param indices
   * @param values
   */
  public SparseDoubleVector(int dim, int[] indices, double[] values) {
    super();
    this.dim = dim;
    this.hashMap = new Int2DoubleOpenHashMap(indices, values);
  }

  /**
   * init the vector by setting the dim
   *
   * @param dim
   */
  public SparseDoubleVector(int dim, Int2DoubleOpenHashMap map) {
    super();
    this.dim = dim;
    this.hashMap = map;
  }

  /**
   * init the vector by another vector
   *
   * @param other
   */
  public SparseDoubleVector(SparseDoubleVector other) {
    super(other);
    this.hashMap = new Int2DoubleOpenHashMap(other.hashMap);

  }

  /**
   * clone the vector
   *
   * @return
   */
  @Override
  public TDoubleVector clone() {
    return new SparseDoubleVector(this);
  }

  /**
   * clone vector by another one
   *
   * @return
   */
  @Override
  public void clone(TVector row) {
    SparseDoubleVector sparseDoubleRow = (SparseDoubleVector) row;
    hashMap.clear();
    hashMap.putAll(sparseDoubleRow.hashMap);
  }

  /**
   * clear the vector
   */
  @Override
  public void clear() {
    if (hashMap != null) {
      hashMap.clear();
    }
  }

  /**
   * calculate the inner product
   *
   * @param other
   * @return
   */
  @Override
  public double dot(TAbstractVector other) {
    if (other instanceof SparseDoubleSortedVector)
      return dot((SparseDoubleSortedVector) other);
    if (other instanceof SparseDoubleVector)
      return dot((SparseDoubleVector) other);
    if (other instanceof DenseDoubleVector)
      return dot((DenseDoubleVector) other);
    if (other instanceof SparseDummyVector)
      return dot((SparseDummyVector) other);

    return 0.0;
  }

  private double dot(DenseDoubleVector other) {
    double ret = 0.0;

    ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      ret += entry.getDoubleValue() * other.values[entry.getIntKey()];
    }

    return ret;
  }

  private double dot(SparseDummyVector other) {
    double ret = 0.0;
    int[] v_indexes = other.indices;
    for (int i = 0; i < other.nonzero; i++) {
      ret += hashMap.get(v_indexes[i]);
    }
    return ret;
  }

  private double dot(SparseDoubleVector other) {
    double ret = 0.0;

    Int2DoubleOpenHashMap smallMap = this.hashMap;
    Int2DoubleOpenHashMap largeMap = other.hashMap;

    if (smallMap.size() > largeMap.size()) {
      smallMap = other.hashMap;
      largeMap = this.hashMap;
    }

    ObjectIterator<Int2DoubleMap.Entry> iter = smallMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      if (largeMap.containsKey(entry.getIntKey())) {
        ret += entry.getDoubleValue() * largeMap.get(entry.getIntKey());
      }
    }
    return ret;
  }

  private double dot(SparseDoubleSortedVector other) {
    ObjectIterator<Int2DoubleMap.Entry> iter = hashMap.int2DoubleEntrySet().fastIterator();

    double ret = 0.0;
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      ret += entry.getDoubleValue() * other.get(entry.getIntKey());
    }
    return ret;
  }

  /**
   * filter the vector and covert to the appropriate type
   *
   * @param x the comparison value
   * @return
   */
  @Override
  public TDoubleVector filter(double x) {
    Int2DoubleOpenHashMap newMap = new Int2DoubleOpenHashMap();

    ObjectIterator<Int2DoubleMap.Entry> iter = hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      double value = entry.getDoubleValue();
      if (Math.abs(value) > x) {
        newMap.put(entry.getIntKey(), value);
      }
    }
    SparseDoubleVector vector = new SparseDoubleVector(dim, newMap);
    vector.setRowId(rowId).setMatrixId(matrixId).setClock(clock);
    return vector;
  }

  /**
   * get the element by index
   *
   * @param index the index
   * @return
   */
  @Override
  public double get(int index) {
    return hashMap.get(index);
  }

  /**
   * get values of all of the elements
   *
   * @return
   */
  @Override
  public double[] getValues() {
    return hashMap.values().toDoubleArray();
  }

  /**
   * get all of the index
   *
   * @return
   */
  @Override
  public int[] getIndices() {
    return hashMap.keySet().toIntArray();
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
   * count the nonzero element
   *
   * @return
   */
  @Override
  public long nonZeroNumber() {
    long ret = 0;
    if (hashMap != null) {
      ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();
      while (iter.hasNext()) {
        if (iter.next().getDoubleValue() != 0) {
          ret++;
        }
      }
    }

    return ret;
  }

  /**
   * plus the vector by another vector
   *
   * @param other the other
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TDoubleVector plus(TAbstractVector other, double x) {
    if (other instanceof SparseDoubleSortedVector)
      return plus((SparseDoubleSortedVector) other, x);
    if (other instanceof SparseDoubleVector)
      return plus((SparseDoubleVector) other, x);
    if (other instanceof DenseDoubleVector)
      return plus((DenseDoubleVector) other, x);

    LOG.error(String.format("Unregistered vector type %s", other.getClass().getName()));
    return null;
  }

  private TDoubleVector plus(DenseDoubleVector other, double x) {
    DenseDoubleVector vec = new DenseDoubleVector(other);
    vec.timesBy(x);

    ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      double delt = vec.get(entry.getIntKey());
      vec.set(entry.getIntKey(), entry.getDoubleValue() + delt);
    }

    return vec;
  }

  private SparseDoubleVector plus(SparseDoubleVector other, double x) {
    SparseDoubleVector vector = (SparseDoubleVector) this.clone();
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.hashMap.addTo(entry.getIntKey(), entry.getDoubleValue() * x);
    }
    return vector;
  }

  private SparseDoubleVector plus(SparseDoubleSortedVector other, double x) {
    SparseDoubleVector vector = (SparseDoubleVector) this.clone();
    int[] otherIndexs = other.indices;
    double[] otherValues = other.values;
    for (int i = 0; i < other.nnz; i++) {
      int key = otherIndexs[i];
      vector.hashMap.addTo(key, otherValues[i] * x);
    }
    return vector;
  }

  @Override
  public TDoubleVector plus(TAbstractVector other) {
    if (other instanceof SparseDoubleSortedVector)
      return plus((SparseDoubleSortedVector) other);
    if (other instanceof SparseDoubleVector)
      return plus((SparseDoubleVector) other);
    if (other instanceof DenseDoubleVector)
      return plus((DenseDoubleVector) other);

    LOG.error(String.format("Unregistered vector type %s", other.getClass().getName()));
    return null;
  }

  public TDoubleVector plus(DenseDoubleVector other) {
    DenseDoubleVector vec = new DenseDoubleVector(other);

    ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      double oldvalue = vec.get(entry.getIntKey());
      vec.set(entry.getIntKey(), entry.getDoubleValue() + oldvalue);
    }

    return vec;
  }

  private SparseDoubleVector plus(SparseDoubleVector other) {
    SparseDoubleVector vector = (SparseDoubleVector) this.clone();

    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.hashMap.addTo(entry.getIntKey(), entry.getDoubleValue());
    }

    return vector;
  }

  private SparseDoubleVector plus(SparseDoubleSortedVector other) {
    SparseDoubleVector vector = (SparseDoubleVector) this.clone();
    int[] otherIndexs = other.indices;
    double[] otherValues = other.values;
    for (int i = 0; i < other.nnz; i++) {
      int key = otherIndexs[i];
      vector.hashMap.addTo(key, otherValues[i]);
    }
    return vector;
  }

  @Override
  public TDoubleVector plus(TAbstractVector other, int x) {
    return plus(other, (double) x);
  }

  @Override
  public SparseDoubleVector plusBy(TAbstractVector other, double x) {
    if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other, x);
    if (other instanceof SparseDoubleVector)
      return plusBy((SparseDoubleVector) other, x);
    if (other instanceof DenseDoubleVector)
      return plusBy((DenseDoubleVector) other, x);
    if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other, x);
    LOG.error(String.format("Unregistered vector type %s", other.getClass().getName()));
    return null;
  }

  private SparseDoubleVector plusBy(DenseDoubleVector other, double x) {
    double[] delta = other.getValues();
    for (int i = 0; i < delta.length; i++) {
      hashMap.addTo(i, delta[i] * x);
    }
    return this;
  }

  private SparseDoubleVector plusBy(int[] index, double[] delta, double x) {
    for (int i = 0; i < index.length; i++) {
      hashMap.addTo(index[i], delta[i] * x);
    }
    return this;
  }

  private SparseDoubleVector plusBy(SparseDummyVector other, double x) {
    int[] v_indexes = other.indices;
    for (int i = 0; i < other.nonzero; i++) {
      hashMap.addTo(v_indexes[i], x);
    }
    return this;
  }

  private SparseDoubleVector plusBy(SparseDoubleVector other, double x) {
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.addTo(entry.getIntKey(), entry.getDoubleValue() * x);
    }
    return this;
  }

  private SparseDoubleVector plusBy(SparseDoubleSortedVector other, double x) {
    return plusBy(other.getIndices(), other.getValues(), x);
  }

  @Override
  public TDoubleVector plusBy(TAbstractVector other, int x) {
    return plusBy(other, (double) x);
  }

  @Override
  public TDoubleVector plusBy(TAbstractVector other) {
    if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other);
    if (other instanceof SparseDoubleVector)
      return plusBy((SparseDoubleVector) other);
    if (other instanceof DenseDoubleVector)
      return plusBy((DenseDoubleVector) other);
    if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other);
    LOG.error(String.format("Unregistered vector type %s", other.getClass().getName()));
    return null;
  }

  private SparseDoubleVector plusBy(DenseDoubleVector other) {
    double[] delta = other.getValues();
    for (int i = 0; i < delta.length; i++) {
      hashMap.addTo(i, delta[i]);
    }
    return this;
  }

  private SparseDoubleVector plusBy(int[] index, double[] delta) {
    for (int i = 0; i < index.length; i++) {
      hashMap.addTo(index[i], delta[i]);
    }
    return this;
  }

  public SparseDoubleVector plusBy(SparseDummyVector other) {
    int[] v_indexes = other.indices;
    for (int i = 0; i < other.nonzero; i++) {
      hashMap.addTo(v_indexes[i], 1.0);
    }
    return this;
  }

  private SparseDoubleVector plusBy(SparseDoubleVector other) {
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.addTo(entry.getIntKey(), entry.getDoubleValue());
    }
    return this;
  }

  private SparseDoubleVector plusBy(SparseDoubleSortedVector other) {
    return plusBy(other.getIndices(), other.getValues());
  }

  /**
   * set the value by index
   *
   * @param index the index
   * @param value the value
   */
  @Override
  public void set(int index, double value) {
    hashMap.put(index, value);
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
   * get the sparsity
   *
   * @return
   */
  @Override
  public double sparsity() {
    return ((double) hashMap.size()) / dim;
  }

  /** w' = w * x */
  @Override
  public TDoubleVector times(double x) {
    SparseDoubleVector vector = (SparseDoubleVector) this.clone();

    ObjectIterator<Int2DoubleMap.Entry> iter = vector.hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.hashMap.put(entry.getIntKey(), entry.getDoubleValue() * x);
    }
    return vector;
  }

  /**
   * the multiplication of vector and element
   *
   * @param x the double multiply factor
   * @return
   */
  @Override
  public TDoubleVector timesBy(double x) {
    ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();

    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.put(entry.getIntKey(), entry.getDoubleValue() * x);
    }
    return this;
  }

  /**
   * get the norm of vector
   *
   * @return
   */
  @Override
  public double squaredNorm() {
    ObjectIterator<Int2DoubleMap.Entry> iter = hashMap.int2DoubleEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      double v = iter.next().getDoubleValue();
      sum += v * v;
    }
    return sum;
  }

}
