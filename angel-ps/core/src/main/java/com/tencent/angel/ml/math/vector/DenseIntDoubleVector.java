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

/**
 * Dense double vector
 */
public class DenseIntDoubleVector extends TIntDoubleVector {

  private final static Log LOG = LogFactory.getLog(DenseIntDoubleVector.class);
  /**
   * the value of vector
   */
  double[] values;

  /**
   * init the vector by another vector
   * 
   * @param other
   */
  public DenseIntDoubleVector(DenseIntDoubleVector other) {
    super(other);
    this.values = new double[this.dim];
    System.arraycopy(other.values, 0, this.values, 0, dim);
  }

  /**
   * init the vector by setting the dim
   * 
   * @param dim
   */
  public DenseIntDoubleVector(int dim) {
    super();
    this.values = new double[dim];
    this.dim = dim;
  }

  /**
   * init the vector by setting the dim and values
   * 
   * @param dim
   * @param values
   */
  public DenseIntDoubleVector(int dim, double[] values) {
    super();
    assert dim == values.length;
    this.dim = dim;
    this.values = values;
  }

  @Override
  public TIntDoubleVector plusBy(int index, double delt) {
    values[index] += delt;
    return this;
  }

  @Override public double sum() {
    double ret = 0.0;
    for (int i = 0; i < dim; i++) {
      ret += this.values[i];
    }
    return ret;
  }

  @Override
  public void clear() {
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        values[i] = 0.0;
      }
    }
  }

  @Override
  public DenseIntDoubleVector clone() {
    return new DenseIntDoubleVector(this);
  }

  @Override
  public void clone(TVector row) {
    super.clone(row);
    System.arraycopy(((DenseIntDoubleVector) row).values, 0, this.values, 0, dim);
  }

  @Override
  public double dot(TAbstractVector other) {
    if (other instanceof SparseIntDoubleSortedVector)
      return dot((SparseIntDoubleSortedVector) other);
    if (other instanceof SparseIntDoubleVector)
      return dot((SparseIntDoubleVector) other);
    if (other instanceof DenseIntDoubleVector)
      return dot((DenseIntDoubleVector) other);
    if (other instanceof SparseDummyVector)
      return dot((SparseDummyVector) other);
    if (other instanceof DenseFloatVector)
      return dot((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return dot((SparseFloatVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " dot " + other.getClass().getName());
  }

  private double dot(DenseIntDoubleVector other) {
    double ret = 0.0;
    for (int i = 0; i < dim; i++) {
      ret += this.values[i] * other.values[i];
    }
    return ret;
  }

  private double dot(DenseFloatVector other) {
    double ret = 0.0;
    for (int i = 0; i < dim; i++) {
      ret += this.values[i] * other.values[i];
    }
    return ret;
  }

  private double dot(SparseDummyVector other) {
    double ret = 0.0;
    for (int i = 0; i < other.nonzero; i++) {
      ret += this.values[other.indices[i]];
    }
    return ret;
  }

  private double dot(SparseIntDoubleVector other) {
    return other.dot(this);
  }

  private double dot(SparseIntDoubleSortedVector other) {
    return other.dot(this);
  }

  private double dot(SparseFloatVector other) { return other.dot(this); }

  @Override
  public TIntDoubleVector filter(double x) {
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
      double[] newValue = new double[nonzero];
      for (int i = 0; i < nonzero; i++) {
        newValue[i] = values[newIndex[i]];
      }

      SparseIntDoubleSortedVector ret = new SparseIntDoubleSortedVector(dim, newIndex, newValue);
      ret.setRowId(rowId).setMatrixId(matrixId).setClock(clock);
      return ret;
    } else {
      return this;
    }
  }

  @Override
  public double get(int index) {
    return values[index];
  }

  @Override
  public double[] getValues() {
    return values;
  }

  @Override
  public MLProtos.RowType getType() {
    return MLProtos.RowType.T_DOUBLE_DENSE;
  }

  @Override
  public int[] getIndices() {
    int [] indices=new int[values.length];
    for(int i=0;i<indices.length;i++)
      indices[i]=i;
    return indices;
  }

  @Override
  public long nonZeroNumber() {
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

  @Override
  public TIntDoubleVector plus(TAbstractVector other, double x) {
    assert dim == other.getDimension();
    if (other instanceof DenseIntDoubleVector)
      return plus((DenseIntDoubleVector) other, x);
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other, x);
    if (other instanceof SparseIntDoubleVector)
      return plus((SparseIntDoubleVector) other, x);
    if (other instanceof SparseIntDoubleSortedVector)
      return plus((SparseIntDoubleSortedVector) other, x);
    if(other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other, x);
    if (other instanceof SparseDummyVector)
      return plus((SparseDummyVector) other, x);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private TIntDoubleVector plus(DenseIntDoubleVector other, double x) {
    DenseIntDoubleVector vector = new DenseIntDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i] * x;
    return vector;
  }

  private TIntDoubleVector plus(DenseFloatVector other, double x) {
    DenseIntDoubleVector vector = new DenseIntDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i] * x;
    return vector;
  }

  private TIntDoubleVector plus(SparseIntDoubleVector other, double x) {
    DenseIntDoubleVector vector = this.clone();
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().iterator();
    Int2DoubleMap.Entry entry = null;
    while(iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getDoubleValue() * x;
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseIntDoubleSortedVector other, double x) {
    DenseIntDoubleVector vector = this.clone();
    int length = other.indices.length;
    for (int i = 0; i < length; i++) {
      vector.values[other.indices[i]] += other.values[i] * x;
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseFloatVector other, double x) {
    DenseIntDoubleVector vector = this.clone();
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().iterator();
    Int2FloatMap.Entry entry = null;
    while(iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getFloatValue() * x;
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseDummyVector other, double x) {
    DenseIntDoubleVector vector = this.clone();
    for (int i = 0; i < other.nonzero; i++) {
      vector.values[other.indices[i]] += x;
    }
    return vector;
  }

  @Override
  public TIntDoubleVector plus(TAbstractVector other) {
    assert dim == other.getDimension();
    if (other instanceof DenseIntDoubleVector)
      return plus((DenseIntDoubleVector) other);
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other);
    if (other instanceof SparseIntDoubleVector)
      return plus((SparseIntDoubleVector) other);
    if (other instanceof SparseIntDoubleSortedVector)
      return plus((SparseIntDoubleSortedVector) other);
    if(other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other);
    if (other instanceof SparseDummyVector)
      return plus((SparseDummyVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private TIntDoubleVector plus(DenseIntDoubleVector other) {
    DenseIntDoubleVector vector = new DenseIntDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i];
    return vector;
  }

  private TIntDoubleVector plus(DenseFloatVector other) {
    DenseIntDoubleVector vector = new DenseIntDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + (double) other.values[i];
    return vector;
  }

  private TIntDoubleVector plus(SparseIntDoubleVector other) {
    DenseIntDoubleVector vector = this.clone();
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getDoubleValue();
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseIntDoubleSortedVector other) {
    DenseIntDoubleVector vector = this.clone();
    int length = other.indices.length;
    for (int i = 0; i < length; i++) {
      vector.values[other.indices[i]] += other.values[i];
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseFloatVector other) {
    DenseIntDoubleVector vector = this.clone();
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getFloatValue();
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseDummyVector other) {
    DenseIntDoubleVector vector = this.clone();
    for (int i = 0; i < other.nonzero; i++) {
      vector.values[other.indices[i]] += 1;
    }
    return vector;
  }

  @Override
  public TIntDoubleVector plusBy(TAbstractVector other, double x) {
    if (other instanceof DenseIntDoubleVector)
      return plusBy((DenseIntDoubleVector) other, x);
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other, x);
    if (other instanceof SparseIntDoubleVector)
      return plusBy((SparseIntDoubleVector) other, x);
    if (other instanceof SparseIntDoubleSortedVector)
      return plusBy((SparseIntDoubleSortedVector) other, x);
    if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other, x);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other, x);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  private TIntDoubleVector plusBy(DenseIntDoubleVector other, double x) {
    double[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i] * x;
    }
    return this;
  }

  private TIntDoubleVector plusBy(DenseFloatVector other, double x) {
    float[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i] * x;
    }
    return this;
  }

  private TIntDoubleVector plusBy(SparseIntDoubleVector other, double x) {
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2DoubleMap.Entry entry = iter.next();
      values[entry.getIntKey()] += entry.getDoubleValue() * x;
    }

    return this;
  }

  private TIntDoubleVector plusBy(SparseIntDoubleSortedVector other, double x) {
    int[] keys = other.getIndices();
    double[] vals = other.getValues();
    for (int i = 0; i < keys.length; i++) {
      values[keys[i]] += vals[i] * x;
    }
    return this;
  }

  private TIntDoubleVector plusBy(SparseDummyVector other, double x) {
    for (int i = 0; i < other.nonzero; i++) {
      this.values[other.indices[i]] += x;
    }
    return this;
  }

  private TIntDoubleVector plusBy(SparseFloatVector other, double x) {
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2FloatMap.Entry entry = iter.next();
      values[entry.getIntKey()] += entry.getFloatValue() * x;
    }
    return this;
  }

  private TIntDoubleVector plusBy(DenseIntDoubleVector other) {
    double[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i];
    }
    return this;
  }

  private TIntDoubleVector plusBy(DenseFloatVector other) {
    float[] delta = other.values;
    for (int i = 0; i < delta.length; i++) {
      values[i] += delta[i];
    }
    return this;
  }

  @Override
  public TIntDoubleVector plusBy(TAbstractVector other) {
    if (other instanceof DenseIntDoubleVector)
      return plusBy((DenseIntDoubleVector) other);
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other);
    if (other instanceof SparseIntDoubleVector)
      return plusBy((SparseIntDoubleVector) other);
    if (other instanceof SparseIntDoubleSortedVector)
      return plusBy((SparseIntDoubleSortedVector) other);
    if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  private TIntDoubleVector plusBy(SparseIntDoubleVector other) {
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      values[entry.getIntKey()] += entry.getDoubleValue();
    }
    return this;
  }

  private TIntDoubleVector plusBy(SparseFloatVector other) {
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      values[entry.getIntKey()] += entry.getFloatValue();
    }
    return this;
  }

  private TIntDoubleVector plusBy(SparseIntDoubleSortedVector other) {
    return plusBy(other.indices, other.getValues());
  }

  private TIntDoubleVector plusBy(SparseDummyVector other) {
    for (int i = 0; i < other.nonzero; i++) {
      this.values[other.indices[i]] += 1.0;
    }
    return this;
  }

  private TIntDoubleVector plusBy(int[] indexes, double[] deltas) {
    int length = indexes.length;
    for (int i = 0; i < length; i++) {
      values[indexes[i]] += deltas[i];
    }
    return this;
  }

  @Override
  public void set(int index, double value) {
    values[index] = value;
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public double sparsity() {
    int nonzero = 0;
    for (int i = 0; i < values.length; i++) {
      if (Math.abs(values[i]) > 0) {
        nonzero += 1;
      }
    }
    return ((double) nonzero) / values.length;
  }

  @Override
  public double squaredNorm() {
    double norm = 0.0;
    for (int i = 0; i < dim; i++)
      norm += values[i] * values[i];
    return norm;
  }

  @Override
  public TIntDoubleVector times(double x) {
    DenseIntDoubleVector vector = new DenseIntDoubleVector(this.dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] * x;
    return vector;
  }

  @Override
  public TIntDoubleVector timesBy(double x) {
    for (int i = 0; i < dim; i++)
      values[i] *= x;
    return this;
  }
}
