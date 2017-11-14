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
import com.tencent.angel.protobuf.generated.MLProtos;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DenseFloatVector extends TFloatVector {

  private final static Log LOG = LogFactory.getLog(DenseFloatVector.class);

  /**
   * the value of vector
   */
  float[] values;

  /**
   * init the vector by another vector
   *
   * @param other
   */
  public DenseFloatVector(DenseFloatVector other) {
    super(other);
    this.values = new float[this.dim];
    System.arraycopy(other.values, 0, this.values, 0, dim);
  }

  /**
   * init the vector by setting the dim
   *
   * @param dim
   */
  public DenseFloatVector(int dim) {
    super();
    this.values = new float[dim];
    this.dim = dim;
  }

  /**
   * init the vector by setting the dim and values
   *
   * @param dim
   * @param values
   */
  public DenseFloatVector(int dim, float[] values) {
    super();
    assert dim == values.length;
    this.dim = dim;
    this.values = values;
  }

  /**
   * clear the vector
   */
  @Override public void clear() {
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        values[i] = 0;
      }
    }
  }

  /**
   * clone the vector
   *
   * @return
   */
  @Override public TFloatVector clone() {
    return new DenseFloatVector(this);
  }

  @Override public TFloatVector plusBy(int index, float delta) {
    values[index] += delta;
    return this;
  }

  @Override public TFloatVector filter(float x) {
    IntArrayList nonzeroIndex = new IntArrayList();
    int nonzero = 0;
    for (int i = 0; i < values.length; i++) {
      if (Math.abs(values[i]) > x) {
        nonzero++;
        nonzeroIndex.add(i);
      }
    }

    if (nonzero < values.length * 0.5) {
      LOG.debug(String.format("Dense Row filter generate a sparse row with nonzero %d", nonzero));
      int[] newIndex = new int[nonzero];
      System.arraycopy(nonzeroIndex.elements(), 0, newIndex, 0, nonzero);
      float[] newValue = new float[nonzero];
      for (int i = 0; i < nonzero; i++) {
        newValue[i] = values[newIndex[i]];
      }

      SparseFloatVector ret = new SparseFloatVector(dim, newIndex, newValue);
      ret.setRowId(rowId).setMatrixId(matrixId).setClock(clock);
      return ret;
    } else {
      return this;
    }
  }

  @Override public TFloatVector times(float x) {
    DenseFloatVector vector = new DenseFloatVector(this);
    for (int i = 0; i < dim; i++) {
      vector.values[i] *= x;
    }
    return vector;
  }

  @Override public TFloatVector timesBy(float x) {
    for (int i = 0; i < dim; i++) {
      values[i] *= x;
    }
    return this;
  }

  /**
   * clone vector by another one
   *
   * @return
   */
  @Override public void clone(TVector row) {
    super.clone(row);
    System.arraycopy(((DenseFloatVector) row).values, 0, this.values, 0, dim);
  }

  @Override public double dot(TAbstractVector other) {
    assert dim == other.getDimension();
    if (other instanceof DenseFloatVector)
      return dot((DenseFloatVector) other);
    if (other instanceof DenseDoubleVector)
      return dot((DenseDoubleVector) other);
    if (other instanceof SparseFloatVector)
      return dot((SparseFloatVector) other);
    if (other instanceof SparseDoubleVector)
      return dot((SparseDoubleVector) other);
    if (other instanceof SparseDoubleSortedVector)
      return dot((SparseDoubleSortedVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: dot " + this.getClass().getName() + " with " + other.getClass()
        .getName());
  }

  private double dot(DenseFloatVector other) {
    double ret = 0.0;
    for (int i = 0; i < dim; i++) {
      ret += this.values[i] * other.values[i];
    }
    return ret;
  }

  private double dot(DenseDoubleVector other) {
    return other.dot(this);
  }

  private double dot(SparseFloatVector other) {
    double ret = 0.0;
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2FloatMap.Entry entry = iter.next();
      ret += values[entry.getIntKey()] * entry.getFloatValue();
    }
    return ret;
  }

  private double dot(SparseDoubleSortedVector other) {
    double ret = 0.0;

    int[] keys = other.getIndices();
    double[] vals = other.getValues();
    for (int i = 0; i < keys.length; i++) {
      ret += values[keys[i]] * (float) vals[i];
    }
    return ret;
  }

  private double dot(SparseDoubleVector other) {
    double ret = 0.0;
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2DoubleMap.Entry entry = iter.next();
      ret += values[entry.getIntKey()] * entry.getDoubleValue();
    }
    return ret;
  }

  @Override public float get(int index) {
    return values[index];
  }

  @Override public float[] getValues() {
    return values;
  }

  @Override public MLProtos.RowType getType() {
    return MLProtos.RowType.T_FLOAT_DENSE;
  }

  @Override public int[] getIndices() {
    int[] indices = new int[values.length];
    for (int i = 0; i < indices.length; i++)
      indices[i] = i;
    return indices;
  }

  @Override public long nonZeroNumber() {
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

  @Override public TVector plus(TAbstractVector other) {
    assert dim == other.getDimension();
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: plus " + this.getClass().getName() + " and " + other.getClass()
        .getName());
  }

  private TVector plus(DenseDoubleVector other) {
    return other.plus(this);
  }

  private TFloatVector plus(DenseFloatVector other) {
    DenseFloatVector vector = new DenseFloatVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i];
    return vector;
  }

  private TFloatVector plus(SparseFloatVector other) {
    DenseFloatVector vector = new DenseFloatVector(this);
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getFloatValue();
    }
    return vector;
  }

  private TVector plus(SparseDoubleVector other) {
    DenseFloatVector vector = new DenseFloatVector(dim);
    System.arraycopy(values, 0, vector.values, 0, dim);

    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2DoubleMap.Entry entry = iter.next();
      vector.values[entry.getIntKey()] += (float) entry.getDoubleValue();
    }
    return vector;
  }

  private TFloatVector plus(SparseDoubleSortedVector other) {
    assert dim == other.getDimension();
    DenseFloatVector vector = new DenseFloatVector(dim);
    System.arraycopy(values, 0, vector.values, 0, dim);

    int[] keys = other.getIndices();
    double[] vals = other.getValues();
    for (int i = 0; i < keys.length; i++) {
      vector.values[keys[i]] += (float) vals[i];
    }
    return vector;
  }

  @Override public TVector plus(TAbstractVector other, float x) {
    assert dim == other.getDimension();
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other, x);
    if (other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other, x);

    throw new UnsupportedOperationException(
      "Unsupportted operation: plus " + this.getClass().getName() + " and " + other.getClass()
        .getName());
  }

  private TFloatVector plus(DenseDoubleVector other, float x) {
    assert dim == other.size();
    DenseFloatVector vector = new DenseFloatVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + (float) (other.values[i] * x);
    return vector;
  }

  private TFloatVector plus(DenseFloatVector other, float x) {
    assert dim == other.size();
    DenseFloatVector vector = new DenseFloatVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i] * (float) x;
    return vector;
  }

  private TFloatVector plus(SparseFloatVector other, float x) {
    assert dim == other.getDimension();
    DenseFloatVector vector = new DenseFloatVector(dim);
    System.arraycopy(values, 0, vector.values, 0, dim);

    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2FloatMap.Entry entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getFloatValue() * x;
    }
    return vector;
  }

  private TFloatVector plus(SparseDoubleVector other, float x) {
    assert dim == other.getDimension();
    DenseFloatVector vector = new DenseFloatVector(dim);
    System.arraycopy(values, 0, vector.values, 0, dim);

    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2DoubleMap.Entry entry = iter.next();
      vector.values[entry.getIntKey()] += (float) (entry.getDoubleValue() * x);
    }
    return vector;
  }

  private TFloatVector plus(SparseDoubleSortedVector other, float x) {
    assert dim == other.getDimension();
    DenseFloatVector vector = new DenseFloatVector(dim);
    System.arraycopy(values, 0, vector.values, 0, dim);

    int[] keys = other.getIndices();
    double[] vals = other.getValues();
    for (int i = 0; i < keys.length; i++) {
      vector.values[keys[i]] += (float) (vals[i] * x);
    }
    return vector;
  }

  @Override public TVector plusBy(TAbstractVector other) {
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: plus " + this.getClass().getName() + " and " + other.getClass()
        .getName());
  }

  private TFloatVector plusBy(DenseDoubleVector other) {
    double[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += (float) delta[i];
    }
    return this;
  }

  private TFloatVector plusBy(DenseFloatVector other) {
    float[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i];
    }
    return this;
  }

  private TFloatVector plusBy(SparseFloatVector other) {
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2FloatMap.Entry entry = iter.next();
      values[entry.getIntKey()] += entry.getFloatValue();
    }

    return this;
  }

  private TFloatVector plusBy(SparseDoubleSortedVector other) {
    int[] keys = other.getIndices();
    double[] vals = other.getValues();
    for (int i = 0; i < keys.length; i++) {
      values[keys[i]] += (float) vals[i];
    }

    return this;
  }

  private TFloatVector plusBy(SparseDoubleVector other) {
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2DoubleMap.Entry entry = iter.next();
      values[entry.getIntKey()] += (float) entry.getDoubleValue();
    }

    return this;
  }

  @Override public TFloatVector plusBy(TAbstractVector other, float x) {
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other, x);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other, x);

    throw new UnsupportedOperationException(
      "Unsupportted operation: plus " + this.getClass().getName() + " and " + other.getClass()
        .getName());
  }

  @Override public double sum() {
    double ret = 0.0;
    for (int i = 0; i < dim; i++) {
      ret += this.values[i];
    }
    return ret;
  }


  private TFloatVector plusBy(DenseDoubleVector other, float x) {
    double[] delts = other.getValues();
    for (int i = 0; i < delts.length; i++) {
      this.values[i] += (float) (delts[i] * x);
    }
    return this;
  }

  private TFloatVector plusBy(DenseFloatVector other, float x) {
    float fx = (float) x;
    float[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i] * fx;
    }
    return this;
  }

  private TFloatVector plusBy(SparseDoubleVector other, float x) {
    float fx = (float) x;
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2DoubleMap.Entry entry = iter.next();
      values[entry.getIntKey()] += (float) entry.getDoubleValue() * fx;
    }

    return this;
  }

  private TFloatVector plusBy(SparseDoubleSortedVector other, float x) {
    float fx = (float) x;
    int[] keys = other.getIndices();
    double[] vals = other.getValues();
    for (int i = 0; i < keys.length; i++) {
      values[keys[i]] += (float) vals[i] * fx;
    }
    return this;
  }

  private TFloatVector plusBy(SparseFloatVector other, float x) {
    float fx = (float) x;
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2FloatMap.Entry entry = iter.next();
      values[entry.getIntKey()] += fx * entry.getFloatValue();
    }

    return this;
  }

  private TFloatVector plusBy(int[] indexes, double[] deltas, float x) {
    float fx = (float) x;
    int length = indexes.length;
    for (int i = 0; i < length; i++) {
      values[indexes[i]] += deltas[i] * fx;
    }
    return this;
  }

  private TFloatVector plusBy(int[] indexes, float[] deltas, float x) {
    float fx = (float) x;
    int length = indexes.length;
    for (int i = 0; i < length; i++) {
      values[indexes[i]] += deltas[i] * fx;
    }
    return this;
  }

  private TFloatVector plusBy(int[] indexes, float[] deltas) {
    int length = indexes.length;
    for (int i = 0; i < length; i++) {
      values[indexes[i]] += deltas[i];
    }
    return this;
  }

  @Override public TFloatVector set(int index, float value) {
    values[index] = value;
    return this;
  }

  @Override public int size() {
    return values.length;
  }

  @Override public double sparsity() {
    int nonzero = 0;
    for (int i = 0; i < values.length; i++) {
      if (Math.abs(values[i]) > 0) {
        nonzero += 1;
      }
    }
    return ((double) nonzero) / values.length;
  }

  @Override public double squaredNorm() {
    double square = 0.0;
    for (int i = 0; i < dim; i++)
      square += values[i] * values[i];
    return square;
  }
}
