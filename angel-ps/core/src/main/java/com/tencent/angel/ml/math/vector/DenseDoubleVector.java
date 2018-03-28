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

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Dense double vector
 */
public class DenseDoubleVector extends TIntDoubleVector implements Serialize{

  private final static Log LOG = LogFactory.getLog(DenseDoubleVector.class);
  /**
   * the value of vector
   */
  double[] values;

  /**
   * init the vector by another vector
   * 
   * @param other
   */
  public DenseDoubleVector(DenseDoubleVector other) {
    super(other);
    this.values = new double[this.dim];
    System.arraycopy(other.values, 0, this.values, 0, dim);
  }

  /**
   * init the vector by setting the dim
   * 
   * @param dim
   */
  public DenseDoubleVector(int dim) {
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
  public DenseDoubleVector(int dim, double[] values) {
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
  public TIntDoubleVector elemUpdate(IntDoubleElemUpdater updater, ElemUpdateParam param) {
    for (int i = 0; i < dim; i++) {
      updater.action(i, values[i], param);
    }
    return this;
  }

  @Override
  public void clear() {
    if (values != null) {
      Arrays.fill(values, 0.0);
    }
  }

  @Override
  public DenseDoubleVector clone() {
    return new DenseDoubleVector(this);
  }

  @Override
  public void clone(TVector row) {
    super.clone(row);
    System.arraycopy(((DenseDoubleVector) row).values, 0, this.values, 0, dim);
  }

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
    if (other instanceof DenseFloatVector)
      return dot((DenseFloatVector) other);
    if (other instanceof SparseFloatVector)
      return dot((SparseFloatVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " dot " + other.getClass().getName());
  }

  private double dot(DenseDoubleVector other) {
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

  private double dot(SparseDoubleVector other) {
    return other.dot(this);
  }

  private double dot(SparseDoubleSortedVector other) {
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

      SparseDoubleSortedVector ret = new SparseDoubleSortedVector(dim, newIndex, newValue);
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
  public RowType getType() {
    return RowType.T_DOUBLE_DENSE;
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
    if (other instanceof DenseDoubleVector)
      return plus((DenseDoubleVector) other, x);
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other, x);
    if (other instanceof SparseDoubleVector)
      return plus((SparseDoubleVector) other, x);
    if (other instanceof SparseDoubleSortedVector)
      return plus((SparseDoubleSortedVector) other, x);
    if(other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other, x);
    if (other instanceof SparseDummyVector)
      return plus((SparseDummyVector) other, x);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private TIntDoubleVector plus(DenseDoubleVector other, double x) {
    DenseDoubleVector vector = new DenseDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i] * x;
    return vector;
  }

  private TIntDoubleVector plus(DenseFloatVector other, double x) {
    DenseDoubleVector vector = new DenseDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i] * x;
    return vector;
  }

  private TIntDoubleVector plus(SparseDoubleVector other, double x) {
    DenseDoubleVector vector = this.clone();
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().iterator();
    Int2DoubleMap.Entry entry = null;
    while(iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getDoubleValue() * x;
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseDoubleSortedVector other, double x) {
    DenseDoubleVector vector = this.clone();
    int length = other.indices.length;
    for (int i = 0; i < length; i++) {
      vector.values[other.indices[i]] += other.values[i] * x;
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseFloatVector other, double x) {
    DenseDoubleVector vector = this.clone();
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().iterator();
    Int2FloatMap.Entry entry = null;
    while(iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getFloatValue() * x;
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseDummyVector other, double x) {
    DenseDoubleVector vector = this.clone();
    for (int i = 0; i < other.nonzero; i++) {
      vector.values[other.indices[i]] += x;
    }
    return vector;
  }

  @Override
  public TIntDoubleVector plus(TAbstractVector other) {
    assert dim == other.getDimension();
    if (other instanceof DenseDoubleVector)
      return plus((DenseDoubleVector) other);
    if (other instanceof DenseFloatVector)
      return plus((DenseFloatVector) other);
    if (other instanceof SparseDoubleVector)
      return plus((SparseDoubleVector) other);
    if (other instanceof SparseDoubleSortedVector)
      return plus((SparseDoubleSortedVector) other);
    if(other instanceof SparseFloatVector)
      return plus((SparseFloatVector) other);
    if (other instanceof SparseDummyVector)
      return plus((SparseDummyVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private TIntDoubleVector plus(DenseDoubleVector other) {
    DenseDoubleVector vector = new DenseDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + other.values[i];
    return vector;
  }

  private TIntDoubleVector plus(DenseFloatVector other) {
    DenseDoubleVector vector = new DenseDoubleVector(dim);
    for (int i = 0; i < dim; i++)
      vector.values[i] = values[i] + (double) other.values[i];
    return vector;
  }

  private TIntDoubleVector plus(SparseDoubleVector other) {
    DenseDoubleVector vector = this.clone();
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    Int2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getDoubleValue();
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseDoubleSortedVector other) {
    DenseDoubleVector vector = this.clone();
    int length = other.indices.length;
    for (int i = 0; i < length; i++) {
      vector.values[other.indices[i]] += other.values[i];
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseFloatVector other) {
    DenseDoubleVector vector = this.clone();
    ObjectIterator<Int2FloatMap.Entry> iter = other.hashMap.int2FloatEntrySet().fastIterator();
    Int2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.values[entry.getIntKey()] += entry.getFloatValue();
    }
    return vector;
  }

  private TIntDoubleVector plus(SparseDummyVector other) {
    DenseDoubleVector vector = this.clone();
    for (int i = 0; i < other.nonzero; i++) {
      vector.values[other.indices[i]] += 1;
    }
    return vector;
  }

  @Override
  public TIntDoubleVector plusBy(TAbstractVector other, double x) {
    if (other instanceof DenseDoubleVector)
      return plusBy((DenseDoubleVector) other, x);
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other, x);
    if (other instanceof SparseDoubleVector)
      return plusBy((SparseDoubleVector) other, x);
    if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other, x);
    if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other, x);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other, x);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  private TIntDoubleVector plusBy(DenseDoubleVector other, double x) {
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

  private TIntDoubleVector plusBy(SparseDoubleVector other, double x) {
    ObjectIterator<Int2DoubleMap.Entry> iter = other.hashMap.int2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      Int2DoubleMap.Entry entry = iter.next();
      values[entry.getIntKey()] += entry.getDoubleValue() * x;
    }

    return this;
  }

  private TIntDoubleVector plusBy(SparseDoubleSortedVector other, double x) {
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

  private TIntDoubleVector plusBy(DenseDoubleVector other) {
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
    if (other instanceof DenseDoubleVector)
      return plusBy((DenseDoubleVector) other);
    if (other instanceof DenseFloatVector)
      return plusBy((DenseFloatVector) other);
    if (other instanceof SparseDoubleVector)
      return plusBy((SparseDoubleVector) other);
    if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other);
    if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other);
    if (other instanceof SparseFloatVector)
      return plusBy((SparseFloatVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  private TIntDoubleVector plusBy(SparseDoubleVector other) {
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

  private TIntDoubleVector plusBy(SparseDoubleSortedVector other) {
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

  @Override public double norm() {
    double norm = 0.0;
    for (int i = 0; i < dim; i++)
      norm += Math.abs(values[i]);
    return norm;
  }

  @Override
  public TIntDoubleVector times(double x) {
    DenseDoubleVector vector = new DenseDoubleVector(this.dim);
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

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(dim);
    buf.writeInt(values.length);
    IntStream.range(0,values.length).forEach(i->buf.writeDouble(values[i]));
  }

  @Override
  public void deserialize(ByteBuf buf) {
    int dim = buf.readInt();
    int length = buf.readInt();
    double[] data = new double[length];
    IntStream.range(0,length).forEach(i->data[i] = buf.readDouble());
    this.dim = dim;
    this.values = data;
  }

  @Override
  public int bufferLen() {
    return 4 + 8 * values.length;
  }
}
