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
import com.tencent.angel.protobuf.generated.MLProtos;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
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
    if(capacity > 0) {
      this.hashMap = new Int2DoubleOpenHashMap(capacity);
    } else {
      this.hashMap = new Int2DoubleOpenHashMap(INIT_SIZE);
    }
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
    assert indices.length == values.length;
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

  @Override
  public TDoubleVector plusBy(int index, double delta) {
    hashMap.addTo(index, delta);
    return this;
  }

  @Override public double sum() {
    double ret = 0.0;
    ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      ret += entry.getDoubleValue();
    }
    return ret;
  }

  @Override public TDoubleVector clone() {
    return new SparseDoubleVector(this);
  }

  @Override
  public void clone(TVector row) {
    super.clone(row);
    hashMap.clear();
    hashMap.putAll(((SparseDoubleVector) row).hashMap);
  }

  @Override
  public void clear() {
    if (hashMap != null) {
      hashMap.clear();
    }
  }

  @Override
  public double dot(TAbstractVector other) {
    assert (dim == other.getDimension());
    if (other instanceof SparseDoubleSortedVector)
      return dot((SparseDoubleSortedVector) other);
    if (other instanceof SparseDoubleVector)
      return dot((SparseDoubleVector) other);
    if (other instanceof DenseDoubleVector)
      return dot((DenseDoubleVector) other);
    if (other instanceof SparseDummyVector)
      return dot((SparseDummyVector) other);
    if (other instanceof DenseFloatVector)
      return dot((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return dot((SparseFloatVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " dot " + other.getClass().getName());
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
    int[] indexes = other.indices;
    for (int i = 0; i < other.nonzero; i++) {
      ret += hashMap.get(indexes[i]);
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

  private double dot(SparseFloatVector other) {
    return other.dot(this);
  }

  private double dot(DenseFloatVector other) {
    double ret = 0.0;
    ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      ret += entry.getDoubleValue() * other.values[entry.getIntKey()];
    }
    return ret;
  }

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

  @Override public double get(int index) {
    return hashMap.get(index);
  }

  @Override public double[] getValues() {
    return hashMap.values().toDoubleArray();
  }

  @Override public int[] getIndices() {
    return hashMap.keySet().toIntArray();
  }

  @Override public MLProtos.RowType getType() {
    return MLProtos.RowType.T_DOUBLE_SPARSE;
  }

  @Override public long nonZeroNumber() {
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

  @Override
  public TDoubleVector plus(TAbstractVector other, double x) {
    assert (dim == other.getDimension());
    if (other instanceof SparseDoubleSortedVector)
      return plus((SparseDoubleSortedVector) other, x);
    if (other instanceof SparseDoubleVector)
      return plus((SparseDoubleVector) other, x);
    if (other instanceof DenseDoubleVector)
      return plus((DenseDoubleVector) other, x);
    if (other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other, x);
    if(other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other, x);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private DenseDoubleVector plus(DenseDoubleVector other, double x) {
    DenseDoubleVector vec = new DenseDoubleVector(other);
    vec.timesBy(x);

    ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vec.plusBy(entry.getIntKey(), entry.getDoubleValue());
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
      vector.hashMap.addTo(otherIndexs[i], otherValues[i] * x);
    }
    return vector;
  }

  private SparseDoubleVector plus(SparseFloatVector other, double x) {
    SparseDoubleVector vector = (SparseDoubleVector) this.clone();

    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.hashMap.addTo(entry.getIntKey(), entry.getFloatValue() * x);
    }
    return vector;
  }

  private DenseDoubleVector plus(DenseFloatVector other, double x) {
    DenseDoubleVector vector = new DenseDoubleVector(dim);
    vector.plusBy(other);
    ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getDoubleValue() * x;
    }
    return vector;
  }

  @Override public TDoubleVector plus(TAbstractVector other) {
    assert (dim == other.getDimension());
    if (other instanceof SparseDoubleSortedVector)
      return plus((SparseDoubleSortedVector) other);
    if (other instanceof SparseDoubleVector)
      return plus((SparseDoubleVector) other);
    if (other instanceof DenseDoubleVector)
      return plus((DenseDoubleVector) other);
    if (other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other);
    if(other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  public TDoubleVector plus(DenseDoubleVector other) {
    DenseDoubleVector vec = new DenseDoubleVector(other);

    ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vec.plusBy(entry.getIntKey(), entry.getDoubleValue());
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
      vector.hashMap.addTo(otherIndexs[i], otherValues[i]);
    }
    return vector;
  }

  private SparseDoubleVector plus(SparseFloatVector other) {
    SparseDoubleVector vector = (SparseDoubleVector) this.clone();

    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.hashMap.addTo(entry.getIntKey(), entry.getFloatValue());
    }
    return vector;
  }

  private DenseDoubleVector plus(DenseFloatVector other) {
    DenseDoubleVector vector = new DenseDoubleVector(dim);
    vector.plusBy(other);
    ObjectIterator<Int2DoubleMap.Entry> iter = this.hashMap.int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getDoubleValue();
    }
    return vector;
  }

  @Override public SparseDoubleVector plusBy(TAbstractVector other, double x) {
    assert (dim == other.getDimension());
    if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other, x);
    if (other instanceof SparseDoubleVector)
      return plusBy((SparseDoubleVector) other, x);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other, x);
    if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other, x);
    if (other instanceof DenseDoubleVector)
      return plusBy((DenseDoubleVector) other, x);
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other, x);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  private SparseDoubleVector plusBy(int[] index, double[] delta, double x) {
    for (int i = 0; i < index.length; i++) {
      hashMap.addTo(index[i], delta[i] * x);
    }
    return this;
  }

  private SparseDoubleVector plusBy(SparseDummyVector other, double x) {
    int[] indexes = other.indices;
    for (int i = 0; i < other.nonzero; i++) {
      hashMap.addTo(indexes[i], x);
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

  private SparseDoubleVector plusBy(SparseFloatVector other, double x) {
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.addTo(entry.getIntKey(), entry.getFloatValue() * x);
    }
    return this;
  }

  private SparseDoubleVector plusBy(SparseDoubleSortedVector other, double x) {
    return plusBy(other.getIndices(), other.getValues(), x);
  }

  private SparseDoubleVector plusBy(DenseDoubleVector other, double x) {
    double[] delta = other.getValues();
    for (int i = 0; i < delta.length; i++) {
      hashMap.addTo(i, delta[i] * x);
    }
    return this;
  }

  private SparseDoubleVector plusBy(DenseFloatVector other, double x) {
    float[] delta = other.getValues();
    for (int i = 0; i < delta.length; i++) {
      hashMap.addTo(i, delta[i] * x);
    }
    return this;
  }

  @Override public TDoubleVector plusBy(TAbstractVector other) {
    assert (dim == other.getDimension());
    if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other);
    if (other instanceof SparseDoubleVector)
      return plusBy((SparseDoubleVector) other);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other);
    if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other);
    if (other instanceof DenseDoubleVector)
      return plusBy((DenseDoubleVector) other);
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  private SparseDoubleVector plusBy(DenseDoubleVector other) {
    double[] delta = other.getValues();
    for (int i = 0; i < delta.length; i++) {
      hashMap.addTo(i, delta[i]);
    }
    return this;
  }

  private SparseDoubleVector plusBy(DenseFloatVector other) {
    float[] delta = other.getValues();
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
    int[] indexes = other.indices;
    for (int i = 0; i < other.nonzero; i++) {
      hashMap.addTo(indexes[i], 1.0);
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

  private SparseDoubleVector plusBy(SparseFloatVector other) {
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.addTo(entry.getIntKey(), entry.getFloatValue());
    }
    return this;
  }

  private SparseDoubleVector plusBy(SparseDoubleSortedVector other) {
    return plusBy(other.getIndices(), other.getValues());
  }

  @Override public void set(int index, double value) {
    hashMap.put(index, value);
  }

  @Override public int size() {
    return hashMap.size();
  }

  @Override public double sparsity() {
    return ((double) nonZeroNumber()) / dim;
  }

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

  public Int2DoubleOpenHashMap getIndexToValueMap() {
    return hashMap;
  }
}
