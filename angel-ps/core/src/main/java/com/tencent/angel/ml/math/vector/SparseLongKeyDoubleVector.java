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
 *
 */

package com.tencent.angel.ml.math.vector;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.protobuf.generated.MLProtos;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.stream.IntStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sparse double vector with long key.
 */
public class SparseLongKeyDoubleVector extends TLongDoubleVector implements Serialize{
  private static final Log LOG = LogFactory.getLog(SparseLongKeyDoubleVector.class);
  /** A (long->double) map */
  private volatile Long2DoubleOpenHashMap indexToValueMap;

  public static final int INIT_SIZE = 1024;

  private volatile long modelNnz;
  /**
   * Init the empty vector
   */
  public SparseLongKeyDoubleVector() {
    this(-1, -1);
  }

  /**
   * Init the vector by setting the dimension
   *
   * @param dim vector dimension
   */
  public SparseLongKeyDoubleVector(long dim) {
    this(dim, -1);
  }

  /**
   * Init the dim and capacity for vector
   *
   * @param dim      vector dimension
   * @param capacity map initialization size
   */
  public SparseLongKeyDoubleVector(long dim, int capacity) {
    super(dim);
    if(capacity <= 0) {
      this.indexToValueMap = new Long2DoubleOpenHashMap(INIT_SIZE);
    } else {
      this.indexToValueMap = new Long2DoubleOpenHashMap(capacity);
    }
  }

  /**
   * Init the vector by setting the dimension , indexes and values
   *
   * @param dim     vector dimension
   * @param indexes value indexes
   * @param values  values
   */
  public SparseLongKeyDoubleVector(long dim, long[] indexes, double[] values) {
    super(dim);
    assert indexes.length == values.length;
    this.indexToValueMap = new Long2DoubleOpenHashMap(indexes, values);
  }

  /**
   * Init the vector by setting the dimension
   *
   * @param dim vector dimension
   * @param map a (long->double) map
   */
  public SparseLongKeyDoubleVector(long dim, Long2DoubleOpenHashMap map) {
    super(dim);
    this.indexToValueMap = map;
  }

  /**
   * Init the vector by another vector
   *
   * @param other other vector
   */
  public SparseLongKeyDoubleVector(SparseLongKeyDoubleVector other) {
    super(other.getLongDim());
    this.matrixId = other.matrixId;
    this.rowId = other.rowId;
    this.clock = other.clock;
    this.indexToValueMap = new Long2DoubleOpenHashMap(other.indexToValueMap);
  }

  @Override public TVector plusBy(TAbstractVector other) {
    if (other instanceof SparseLongKeyDoubleVector)
      return plusBy((SparseLongKeyDoubleVector) other);
    else if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other);
    else if (other instanceof SparseLongKeySortedDoubleVector)
      return plusBy((SparseLongKeySortedDoubleVector) other);
    else if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other);
    else if (other instanceof SparseLongKeyDummyVector)
      return plusBy((SparseLongKeyDummyVector) other);
    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private SparseLongKeyDoubleVector plusBy(SparseLongKeyDoubleVector other) {
    if(indexToValueMap.size() == 0) {
      indexToValueMap = other.indexToValueMap.clone();
    } else if(indexToValueMap.size() < other.size()) {
      Long2DoubleOpenHashMap oldMap = indexToValueMap;
      indexToValueMap = other.indexToValueMap.clone();

      ObjectIterator<Long2DoubleMap.Entry> iter =
        oldMap.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue());
      }
    } else {
      ObjectIterator<Long2DoubleMap.Entry> iter =
        other.indexToValueMap.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue());
      }
    }

    return this;
  }

  private SparseLongKeyDoubleVector plusBy(SparseDoubleSortedVector other) {
    resize(other.size());
    int [] indexes = other.getIndices();
    double [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], values[i]);
    }

    return this;
  }

  private SparseLongKeyDoubleVector plusBy(SparseLongKeySortedDoubleVector other) {
    resize(other.size());
    long [] indexes = other.getIndexes();
    double [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], values[i]);
    }

    return this;
  }

  private SparseLongKeyDoubleVector plusBy(SparseDummyVector other) {
    resize(other.size());
    int [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], 1);
    }

    return this;
  }

  private SparseLongKeyDoubleVector plusBy(SparseLongKeyDummyVector other) {
    resize(other.size());
    long [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], 1);
    }

    return this;
  }

  private double sum(SparseLongKeyDoubleVector row) {
    double [] data = row.getValues();
    double ret = 0.0;
    for(int i = 0; i < data.length; i++) {
      ret += data[i];
    }

    return ret;
  }

  @Override public TVector plusBy(long index, double x) {
    indexToValueMap.addTo(index, x);
    return this;
  }

  @Override public TVector plusBy(TAbstractVector other, double x) {
    if (other instanceof SparseLongKeyDoubleVector)
      return plusBy((SparseLongKeyDoubleVector) other, x);
    else if (other instanceof SparseDoubleSortedVector)
      return plusBy((SparseDoubleSortedVector) other, x);
    else if (other instanceof SparseLongKeySortedDoubleVector)
      return plusBy((SparseLongKeySortedDoubleVector) other, x);
    else if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other, x);
    else if (other instanceof SparseLongKeyDummyVector)
      return plusBy((SparseLongKeyDummyVector) other, x);
    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private SparseLongKeyDoubleVector plusBy(SparseLongKeyDoubleVector other, double x) {
    if(this.indexToValueMap.isEmpty()) {
      this.indexToValueMap.putAll(other.getIndexToValueMap());
    } else {
      resize(other.size());

      ObjectIterator<Long2DoubleMap.Entry> iter =
        other.indexToValueMap.long2DoubleEntrySet().fastIterator();
      Long2DoubleMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue() * x);
      }
    }

    return this;
  }

  private void resize(int newSize) {
    if(indexToValueMap.size() < newSize) {
      Long2DoubleOpenHashMap oldMap = indexToValueMap;
      indexToValueMap = new Long2DoubleOpenHashMap(newSize);
      indexToValueMap.putAll(oldMap);
    }
  }

  private SparseLongKeyDoubleVector plusBy(SparseDoubleSortedVector other, double x) {
    resize(other.size());

    int [] indexes = other.getIndices();
    double [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], values[i] * x);
    }

    return this;
  }

  private SparseLongKeyDoubleVector plusBy(SparseLongKeySortedDoubleVector other, double x) {
    resize(other.size());

    long [] indexes = other.getIndexes();
    double [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], values[i] * x);
    }

    return this;
  }

  private SparseLongKeyDoubleVector plusBy(SparseDummyVector other, double x) {
    resize(other.size());

    int [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], x);
    }

    return this;
  }

  private SparseLongKeyDoubleVector plusBy(SparseLongKeyDummyVector other, double x) {
    resize(other.size());
    long [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], x);
    }

    return this;
  }

  @Override public TVector plus(TAbstractVector other) {
    if (other instanceof SparseLongKeyDoubleVector)
      return plus((SparseLongKeyDoubleVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plus " + other.getClass()
        .getName());
  }

  private SparseLongKeyDoubleVector plus(SparseLongKeyDoubleVector other) {
    SparseLongKeyDoubleVector baseVector = null;
    SparseLongKeyDoubleVector streamVector = null;
    if (size() < other.size()) {
      baseVector = new SparseLongKeyDoubleVector(other);
      streamVector = this;
    } else {
      baseVector = new SparseLongKeyDoubleVector(this);
      streamVector = other;
    }

    ObjectIterator<Long2DoubleMap.Entry> iter =
      streamVector.indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      baseVector.indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue());
    }

    return baseVector;
  }

  @Override public TVector plus(TAbstractVector other, double x) {
    if (other instanceof SparseLongKeyDoubleVector)
      return plus((SparseLongKeyDoubleVector) other, x);
    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plus " + other.getClass()
        .getName());
  }

  private SparseLongKeyDoubleVector plus(SparseLongKeyDoubleVector other, double x) {
    SparseLongKeyDoubleVector baseVector = null;
    SparseLongKeyDoubleVector streamVector = null;
    if (size() < other.size()) {
      baseVector = new SparseLongKeyDoubleVector(other);
      streamVector = this;
    } else {
      baseVector = new SparseLongKeyDoubleVector(this);
      streamVector = other;
    }

    ObjectIterator<Long2DoubleMap.Entry> iter =
      streamVector.indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      baseVector.indexToValueMap.addTo(entry.getLongKey(), entry.getDoubleValue() * x);
    }

    return baseVector;
  }

  @Override public double dot(TAbstractVector other) {
    if (other instanceof SparseLongKeyDoubleVector)
      return dot((SparseLongKeyDoubleVector) other);
    else if (other instanceof SparseDoubleSortedVector)
      return dot((SparseDoubleSortedVector) other);
    else if (other instanceof SparseLongKeySortedDoubleVector)
      return dot((SparseLongKeySortedDoubleVector) other);
    else if (other instanceof SparseDummyVector)
      return dot((SparseDummyVector) other);
    else if (other instanceof SparseLongKeyDummyVector)
      return dot((SparseLongKeyDummyVector) other);
    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " dot " + other.getClass()
        .getName());
  }

  private double dot(SparseLongKeyDoubleVector other) {
    double ret = 0.0;
    if (size() <= other.size()) {
      ObjectIterator<Long2DoubleMap.Entry> iter =
        indexToValueMap.long2DoubleEntrySet().fastIterator();
      while (iter.hasNext()) {
        Long2DoubleMap.Entry entry = iter.next();
        ret += other.get(entry.getLongKey()) * entry.getDoubleValue();
      }
      return ret;
    } else {
      return other.dot(this);
    }
  }

  private double dot(SparseDoubleSortedVector other) {
    int [] indexes = other.getIndices();
    double [] values = other.getValues();
    double ret = 0.0;
    for(int i = 0; i < indexes.length; i++) {
      ret += values[i] * get(indexes[i]);
    }

    return ret;
  }

  private double dot(SparseLongKeySortedDoubleVector other) {
    long [] indexes = other.getIndexes();
    double [] values = other.getValues();
    double ret = 0.0;
    for(int i = 0; i < indexes.length; i++) {
      ret += values[i] * get(indexes[i]);
    }

    return ret;
  }

  private double dot(SparseDummyVector other) {
    int [] indexes = other.getIndices();
    double ret = 0.0;
    for(int i = 0; i < indexes.length; i++) {
      ret += get(indexes[i]);
    }

    return ret;
  }

  private double dot(SparseLongKeyDummyVector other) {
    double ret = 0.0;
    for(long i:  other.getIndices()) {
      ret += get(i);
    }

    return ret;
  }

  public long getModelNnz() {
    return modelNnz;
  }

  public void setModelNnz(long modelNnz) {
    this.modelNnz = modelNnz;
  }

  @Override public double get(long key) {
    return indexToValueMap.get(key);
  }

  @Override public double get(int key) {
    return get((long) key);
  }

  @Override public long[] getIndexes() {
    return indexToValueMap.keySet().toLongArray();
  }

  @Override public double[] getValues() {
    return indexToValueMap.values().toDoubleArray();
  }

  @Override public void set(long key, double value) {
    indexToValueMap.put(key, value);
    //return this;
  }

  @Override public void set(int key, double value) {
    set((long) key, value);
  }


  @Override public TVector times(double x) {
    SparseLongKeyDoubleVector vector = new SparseLongKeyDoubleVector(dim, indexToValueMap.size());
    ObjectIterator<Long2DoubleMap.Entry> iter =
      indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.set(entry.getLongKey(), entry.getDoubleValue() * x);
    }
    return vector;
  }

  @Override public TVector timesBy(double x) {
    ObjectIterator<Long2DoubleMap.Entry> iter =
      indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      entry.setValue(entry.getDoubleValue() * x);
    }
    return this;
  }

  @Override public TVector filter(double x) {
    SparseLongKeyDoubleVector vector = new SparseLongKeyDoubleVector(this.dim);

    ObjectIterator<Long2DoubleMap.Entry> iter =
      indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      double value = entry.getDoubleValue();
      if (Math.abs(value) > x) {
        vector.set(entry.getLongKey(), value);
      }
    }
    vector.setRowId(rowId).setMatrixId(matrixId).setClock(clock);
    return vector;
  }

  @Override public SparseLongKeyDoubleVector clone() {
    return new SparseLongKeyDoubleVector(this);
  }

  @Override public void clear() {
    indexToValueMap.clear();
  }

  @Override public long nonZeroNumber() {
    long counter = 0L;
    ObjectIterator<Long2DoubleMap.Entry> iter =
      indexToValueMap.long2DoubleEntrySet().fastIterator();
    while (iter.hasNext()) {
      if(iter.next().getDoubleValue() != 0.0)
        counter++;
    }

    return counter;
  }

  @Override public void clone(TVector vector) {
    assert vector instanceof SparseLongKeyDoubleVector;

    this.matrixId = ((SparseLongKeyDoubleVector)vector).matrixId;
    this.rowId = ((SparseLongKeyDoubleVector)vector).rowId;
    this.clock = ((SparseLongKeyDoubleVector)vector).clock;
    this.indexToValueMap.clear();
    this.indexToValueMap.putAll(((SparseLongKeyDoubleVector)vector).indexToValueMap);
  }

  @Override public double squaredNorm() {
    ObjectIterator<Long2DoubleMap.Entry> iter = indexToValueMap.long2DoubleEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      double v = iter.next().getDoubleValue();
      sum += v * v;
    }
    return sum;
  }

  @Override public double norm() {
    ObjectIterator<Long2DoubleMap.Entry> iter = indexToValueMap.long2DoubleEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      sum += Math.abs(iter.next().getDoubleValue());
    }
    return sum;
  }

  @Override public double sparsity() {
    return (double)nonZeroNumber() / dim;
  }

  @Override public RowType getType() {
    return RowType.T_DOUBLE_SPARSE_LONGKEY;
  }

  @Override public int size() {
    return indexToValueMap.size();
  }

  public double sum() {
    ObjectIterator<Long2DoubleMap.Entry> iter = indexToValueMap.long2DoubleEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      double v = iter.next().getDoubleValue();
      sum += v;
    }
    return sum;
  }

  public Long2DoubleOpenHashMap getIndexToValueMap() {
    return indexToValueMap;
  }

  @Override
  public TLongDoubleVector elemUpdate(LongDoubleElemUpdater updater, ElemUpdateParam param) {
    ObjectIterator<Long2DoubleMap.Entry> iter = indexToValueMap.long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry;
    while (iter.hasNext()) {
      entry = iter.next();
      entry.setValue(updater.action(entry.getLongKey(), entry.getDoubleValue(), param));
    }
    return null;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeLong(dim);
    buf.writeInt(indexToValueMap.size());
    indexToValueMap.forEach((key, value) -> {
      buf.writeLong(key);
      buf.writeDouble(value);
    });
  }

  @Override
  public void deserialize(ByteBuf buf) {
    int dim = buf.readInt();
    int length = buf.readInt();
    Long2DoubleOpenHashMap data = new Long2DoubleOpenHashMap(dim);
    IntStream.range(0,length).forEach(i->data.put(buf.readLong(), buf.readDouble()));
    this.dim = dim;
    this.indexToValueMap = data;

  }

  @Override
  public int bufferLen() {
    return 12 + (8 + 8) * indexToValueMap.size();
  }
}
