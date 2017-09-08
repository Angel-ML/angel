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
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Dense int vector.
 */
public class DenseIntVector extends TIntVector {

  private final static Log LOG = LogFactory.getLog(DenseIntVector.class);

  /**
   * the value of vector
   */
  int[] values;

  /**
   * init the vector by another vector
   *
   * @param other
   */
  public DenseIntVector(DenseIntVector other) {
    super(other);
    values = new int[dim];
    System.arraycopy(other.values, 0, values, 0, dim);
  }

  /**
   * init the vector by setting the dim
   *
   * @param dim
   */
  public DenseIntVector(int dim) {
    super();
    this.dim = dim;
    values = new int[dim];
  }

  /**
   * init the vector by setting the dim and values
   *
   * @param dim
   * @param values
   */
  public DenseIntVector(int dim, int[] values) {
    super();
    this.dim = dim;
    this.values = values;
  }

  @Override
  public TIntVector plusBy(int index, int x) {
    this.values[index] += x;
    return this;
  }

  @Override public TIntVector filter(int x) {
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
      int[] newValue = new int[nonzero];
      for (int i = 0; i < nonzero; i++) {
        newValue[i] = values[newIndex[i]];
      }

      SparseIntVector ret = new SparseIntVector(dim, newIndex, newValue);
      ret.setRowId(rowId).setMatrixId(matrixId).setClock(clock);
      return ret;
    } else {
      return this;
    }
  }

  @Override public TIntVector times(int x) {
    DenseIntVector vector = new DenseIntVector(this);
    for(int i = 0; i < dim; i++) {
      vector.values[i] *= x;
    }
    return vector;
  }

  @Override public TIntVector timesBy(int x) {
    for(int i = 0; i < dim; i++) {
      values[i] *= x;
    }
    return this;
  }

  @Override
  public TVector clone() {
    return new DenseIntVector(this);
  }

  @Override
  public void clone(TVector row) {
    super.clone(row);
    System.arraycopy(((DenseIntVector) row).values, 0, this.values, 0, dim);
  }

  @Override
  public double squaredNorm() {
    double norm = 0.0;
    for (int i = 0; i < dim; i++)
      norm += values[i] * values[i];
    return norm;
  }

  @Override
  public void clear() {
    if (values != null) {
      for (int i = 0; i < values.length; i++) {
        values[i] = 0;
      }
    }
  }

  @Override
  public double dot(TAbstractVector other) {
    assert (dim == other.getDimension());
    if(other instanceof DenseIntVector)
      dot((DenseIntVector) other);
    if(other instanceof SparseIntVector)
      dot((SparseIntVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " dot " + other.getClass().getName());
  }

  private double dot(DenseIntVector other) {
    long dotValue = 0;
    for(int i = 0; i < dim; i++) {
      dotValue += values[i] * other.values[i];
    }
    return dotValue;
  }

  private double dot(SparseIntVector other) {
    double ret = 0.0;
    ObjectIterator<Int2IntMap.Entry> iter = other.hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      ret += entry.getIntValue() * values[entry.getIntKey()];
    }
    return ret;
  }

  @Override
  public int get(int index) {
    return values[index];
  }

  @Override
  public int[] getValues() {
    return values;
  }

  @Override
  public int[] getIndices() {
    int [] indexes = new int[dim];
    for(int i = 0; i < dim; i++) {
      indexes[i] = i;
    }
    return indexes;
  }

  @Override
  public MLProtos.RowType getType() {
    return  MLProtos.RowType.T_INT_DENSE;
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
  public TVector plusBy(TAbstractVector other) {
    assert (dim == other.getDimension());
    if (other instanceof DenseIntVector)
      return plusBy((DenseIntVector) other);
    if (other instanceof SparseIntVector)
      return plusBy((SparseIntVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  private TIntVector plusBy(DenseIntVector other) {
    for (int i = 0; i < dim; i++)
      values[i] += other.values[i];
    return this;
  }

  private TIntVector plusBy(SparseIntVector other) {
    ObjectIterator<Int2IntMap.Entry> iter = other.hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      values[entry.getIntKey()] += entry.getIntValue();
    }
    return this;
  }

  @Override
  public TIntVector plusBy(TAbstractVector other, int x) {
    assert (dim == other.getDimension());
    if (other instanceof DenseIntVector)
      return plusBy((DenseIntVector) other, x);
    if (other instanceof SparseIntVector)
      return plusBy((SparseIntVector) other, x);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  @Override public long sum() {
    long ret = 0;
    for(int i = 0; i < dim; i++) {
      ret += values[i];
    }
    return ret;
  }

  private TIntVector plusBy(DenseIntVector other, int x) {
    for (int i = 0; i < dim; i++)
      values[i] += other.values[i] * x;
    return this;
  }

  private TIntVector plusBy(SparseIntVector other, int x) {
    ObjectIterator<Int2IntMap.Entry> iter = other.hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      values[entry.getIntKey()] += entry.getIntValue() * x;
    }
    return this;
  }

  @Override
  public TVector plus(TAbstractVector other) {
    assert (dim == other.getDimension());
    if (other instanceof DenseIntVector)
      return plus((DenseIntVector) other);
    if (other instanceof SparseIntVector)
      return plus((SparseIntVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private TIntVector plus(DenseIntVector other) {
    DenseIntVector vector = new DenseIntVector(dim);
    for (int i = 0; i < dim; i++) {
      vector.values[i] = values[i] + other.values[i];
    }
    return vector;
  }

  private TIntVector plus(SparseIntVector other) {
    DenseIntVector vector = new DenseIntVector(this);
    ObjectIterator<Int2IntMap.Entry> iter = other.hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getIntValue();
    }
    return vector;
  }

  @Override
  public TIntVector plus(TAbstractVector other, int x) {
    assert (dim == other.getDimension());
    if (other instanceof DenseIntVector)
      return plus((DenseIntVector) other, x);
    if (other instanceof SparseIntVector)
      return plus((SparseIntVector) other, x);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private TIntVector plus(DenseIntVector other, int x) {
    DenseIntVector vector = new DenseIntVector(dim);
    for (int i = 0; i < dim; i++) {
      vector.values[i] = values[i] + x * other.values[i];
    }
    return vector;
  }

  private TIntVector plus(SparseIntVector other, int x) {
    DenseIntVector vector = new DenseIntVector(this);
    ObjectIterator<Int2IntMap.Entry> iter = other.hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getIntValue() * x;
    }
    return vector;
  }

  @Override
  public TIntVector set(int index, int value) {
    values[index] = value;
    return this;
  }

  @Override
  public double sparsity() {
    return ((double) nonZeroNumber()) / values.length;
  }

  @Override
  public int size() {
    return values.length;
  }
}
