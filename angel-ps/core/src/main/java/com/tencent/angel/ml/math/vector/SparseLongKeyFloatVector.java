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
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2FloatOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.stream.IntStream;

/**
 * Sparse double vector with long key.
 */
public class SparseLongKeyFloatVector extends TLongFloatVector implements Serialize{
  private static final Log LOG = LogFactory.getLog(SparseLongKeyFloatVector.class);
  /** A (long->double) map */
  private volatile Long2FloatOpenHashMap indexToValueMap;

  public static final int INIT_SIZE = 1024 * 1024;

  /**
   * Init the empty vector
   */
  public SparseLongKeyFloatVector() {
    this(-1, -1);
  }

  @Override
  public int[] getIndices() {
    return new int[0];
  }

  /**
   * Init the vector by setting the dimension
   *
   * @param dim vector dimension
   */
  public SparseLongKeyFloatVector(long dim) {
    this(dim, -1);
  }

  /**
   * Init the dim and capacity for vector
   *
   * @param dim      vector dimension
   * @param capacity map initialization size
   */
  public SparseLongKeyFloatVector(long dim, int capacity) {
    super(dim);
    if(capacity <= 0) {
      this.indexToValueMap = new Long2FloatOpenHashMap(INIT_SIZE);
    } else {
      this.indexToValueMap = new Long2FloatOpenHashMap(capacity);
    }
  }

  /**
   * Init the vector by setting the dimension , indexes and values
   *
   * @param dim     vector dimension
   * @param indexes value indexes
   * @param values  values
   */
  public SparseLongKeyFloatVector(long dim, long[] indexes, float[] values) {
    super(dim);
    assert indexes.length == values.length;
    this.indexToValueMap = new Long2FloatOpenHashMap(indexes, values);
  }

  /**
   * Init the vector by setting the dimension
   *
   * @param dim vector dimension
   * @param map a (long->double) map
   */
  public SparseLongKeyFloatVector(long dim, Long2FloatOpenHashMap map) {
    super(dim);
    this.indexToValueMap = map;
  }

  /**
   * Init the vector by another vector
   *
   * @param other other vector
   */
  public SparseLongKeyFloatVector(SparseLongKeyFloatVector other) {
    super(other.getLongDim());
    this.matrixId = other.matrixId;
    this.rowId = other.rowId;
    this.clock = other.clock;
    this.indexToValueMap = new Long2FloatOpenHashMap(other.indexToValueMap);
  }

  @Override public TVector plusBy(TAbstractVector other) {
    if (other instanceof SparseLongKeyFloatVector)
      return plusBy((SparseLongKeyFloatVector) other);
    else if (other instanceof SparseFloatSortedVector)
      return plusBy((SparseFloatSortedVector) other);
    else if (other instanceof SparseLongKeySortedFloatVector)
      return plusBy((SparseLongKeySortedFloatVector) other);
    else if (other instanceof SparseDummyVector)
      return plusBy((SparseDummyVector) other);
    else if (other instanceof SparseLongKeyDummyVector)
      return plusBy((SparseLongKeyDummyVector) other);
    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plusBy " + other.getClass()
        .getName());
  }

  private SparseLongKeyFloatVector plusBy(SparseLongKeyFloatVector other) {
    assert (dim == -1 || dim == other.getLongDim());
    if(indexToValueMap.size() == 0) {
      indexToValueMap = other.indexToValueMap.clone();
    } else if(indexToValueMap.size() < other.size()) {
      Long2FloatOpenHashMap oldMap = indexToValueMap;
      indexToValueMap = other.indexToValueMap.clone();

      ObjectIterator<Long2FloatMap.Entry> iter =
        oldMap.long2FloatEntrySet().fastIterator();
      Long2FloatMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexToValueMap.addTo(entry.getLongKey(), entry.getFloatValue());
      }
    } else {
      ObjectIterator<Long2FloatMap.Entry> iter =
        other.indexToValueMap.long2FloatEntrySet().fastIterator();
      Long2FloatMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexToValueMap.addTo(entry.getLongKey(), entry.getFloatValue());
      }
    }

    return this;
  }

  private SparseLongKeyFloatVector plusBy(SparseFloatSortedVector other) {
    assert (dim == -1 || dim == other.getDimension());
    resize(other.size());
    int [] indexes = other.getIndices();
    float [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], values[i]);
    }

    return this;
  }

  private SparseLongKeyFloatVector plusBy(SparseLongKeySortedFloatVector other) {
    assert (dim == -1 || dim == other.getLongDim());
    resize(other.size());
    long [] indexes = other.getIndexes();
    float[] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], values[i]);
    }

    return this;
  }

  private SparseLongKeyFloatVector plusBy(SparseDummyVector other) {
    assert (dim == -1 || dim == other.getDimension());
    resize(other.size());
    int [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], 1);
    }

    return this;
  }

  private SparseLongKeyFloatVector plusBy(SparseLongKeyDummyVector other) {
    assert (dim == -1 || dim == other.getDimension());
    resize(other.size());
    long [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], 1);
    }

    return this;
  }

  private double sum(SparseLongKeyFloatVector row) {
    float [] data = row.getValues();
    double ret = 0.0;
    for(int i = 0; i < data.length; i++) {
      ret += data[i];
    }

    return ret;
  }

  @Override public TVector plusBy(long index, float x) {
    indexToValueMap.addTo(index, x);
    return this;
  }

  @Override public TVector plusBy(TAbstractVector other, double x) {
    if (other instanceof SparseLongKeyFloatVector)
      return plusBy((SparseLongKeyFloatVector) other, x);
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

  private SparseLongKeyFloatVector plusBy(SparseLongKeyFloatVector other, float x) {
    assert (dim == -1 || dim == other.getLongDim());
    if(this.indexToValueMap.isEmpty()) {
      this.indexToValueMap.putAll(other.getIndexToValueMap());
    } else {
      resize(other.size());

      ObjectIterator<Long2FloatMap.Entry> iter =
        other.indexToValueMap.long2FloatEntrySet().fastIterator();
      Long2FloatMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        indexToValueMap.addTo(entry.getLongKey(), entry.getFloatValue() * x);
      }
    }

    return this;
  }

  private void resize(int newSize) {
    if(indexToValueMap.size() < newSize) {
      Long2FloatOpenHashMap oldMap = indexToValueMap;
      indexToValueMap = new Long2FloatOpenHashMap(newSize);
      indexToValueMap.putAll(oldMap);
    }
  }

  private SparseLongKeyFloatVector plusBy(SparseFloatSortedVector other, float x) {
    assert (dim == -1 || dim == other.getDimension());
    resize(other.size());

    int [] indexes = other.getIndices();
    float [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], values[i] * x);
    }

    return this;
  }

  private SparseLongKeyFloatVector plusBy(SparseLongKeySortedFloatVector other, float x) {
    assert (dim == -1 || dim == other.getLongDim());
    resize(other.size());

    long [] indexes = other.getIndexes();
    float [] values = other.getValues();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], values[i] * x);
    }

    return this;
  }

  private SparseLongKeyFloatVector plusBy(SparseDummyVector other, float x) {
    assert (dim == -1 || dim == other.getDimension());
    resize(other.size());

    int [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], x);
    }

    return this;
  }

  private SparseLongKeyFloatVector plusBy(SparseLongKeyDummyVector other, float x) {
    assert (dim == -1 || dim == other.getDimension());
    resize(other.size());

    long [] indexes = other.getIndices();
    for(int i = 0; i < indexes.length; i++) {
      indexToValueMap.addTo(indexes[i], x);
    }

    return this;
  }

  @Override public TVector plus(TAbstractVector other) {
    if (other instanceof SparseLongKeyFloatVector)
      return plus((SparseLongKeyFloatVector) other);

    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plus " + other.getClass()
        .getName());
  }

  private SparseLongKeyFloatVector plus(SparseLongKeyFloatVector other) {
    assert dim == other.dim;
    SparseLongKeyFloatVector baseVector = null;
    SparseLongKeyFloatVector streamVector = null;
    if (size() < other.size()) {
      baseVector = new SparseLongKeyFloatVector(other);
      streamVector = this;
    } else {
      baseVector = new SparseLongKeyFloatVector(this);
      streamVector = other;
    }

    ObjectIterator<Long2FloatMap.Entry> iter =
      streamVector.indexToValueMap.long2FloatEntrySet().fastIterator();
    Long2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      baseVector.indexToValueMap.addTo(entry.getLongKey(), entry.getFloatValue());
    }

    return baseVector;
  }

  @Override public TVector plus(TAbstractVector other, double x) {
    if (other instanceof SparseLongKeyFloatVector)
      return plus((SparseLongKeyFloatVector) other, x);
    throw new UnsupportedOperationException(
      "Unsupportted operation: " + this.getClass().getName() + " plus " + other.getClass()
        .getName());
  }

  private SparseLongKeyFloatVector plus(SparseLongKeyFloatVector other, float x) {
    assert (dim == -1 || dim == other.getLongDim());
    SparseLongKeyFloatVector baseVector = null;
    SparseLongKeyFloatVector streamVector = null;
    if (size() < other.size()) {
      baseVector = new SparseLongKeyFloatVector(other);
      streamVector = this;
    } else {
      baseVector = new SparseLongKeyFloatVector(this);
      streamVector = other;
    }

    ObjectIterator<Long2FloatMap.Entry> iter =
      streamVector.indexToValueMap.long2FloatEntrySet().fastIterator();
    Long2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      baseVector.indexToValueMap.addTo(entry.getLongKey(), entry.getFloatValue() * x);
    }

    return baseVector;
  }

  @Override public double dot(TAbstractVector other) {
    if (other instanceof SparseLongKeyFloatVector)
      return dot((SparseLongKeyFloatVector) other);
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

  private double dot(SparseLongKeyFloatVector other) {
    assert (dim == -1 || dim == other.getLongDim());
    double ret = 0.0;
    if (size() <= other.size()) {
      ObjectIterator<Long2FloatMap.Entry> iter =
        indexToValueMap.long2FloatEntrySet().fastIterator();
      Long2FloatMap.Entry entry = null;
      while (iter.hasNext()) {
        entry = iter.next();
        ret += other.get(entry.getLongKey()) * entry.getFloatValue();
      }
      return ret;
    } else {
      return other.dot(this);
    }
  }

  private double dot(SparseDoubleSortedVector other) {
    assert (dim == -1 || dim == other.getDimension());
    int [] indexes = other.getIndices();
    double [] values = other.getValues();
    double ret = 0.0;
    for(int i = 0; i < indexes.length; i++) {
      ret += values[i] * get(indexes[i]);
    }

    return ret;
  }

  private double dot(SparseLongKeySortedDoubleVector other) {
    assert (dim == -1 || dim == other.getLongDim());
    long [] indexes = other.getIndexes();
    double [] values = other.getValues();
    double ret = 0.0;
    for(int i = 0; i < indexes.length; i++) {
      ret += values[i] * get(indexes[i]);
    }

    return ret;
  }

  private double dot(SparseDummyVector other) {
    assert (dim == -1 || dim == other.getDimension());
    int [] indexes = other.getIndices();
    double ret = 0.0;
    for(int i = 0; i < indexes.length; i++) {
      ret += get(indexes[i]);
    }

    return ret;
  }

  private double dot(SparseLongKeyDummyVector other) {
    assert (dim == -1 || dim == other.getDimension());
    long [] indexes = other.getIndices();
    double ret = 0.0;
    for(int i = 0; i < indexes.length; i++) {
      ret += get(indexes[i]);
    }

    return ret;
  }

  @Override public float get(long key) {
    return indexToValueMap.get(key);
  }

  @Override public float get(int key) {
    return get((long) key);
  }

  @Override public long[] getIndexes() {
    return indexToValueMap.keySet().toLongArray();
  }

  @Override public float[] getValues() {
    return indexToValueMap.values().toFloatArray();
  }

  @Override public void set(long key, float value) {
    indexToValueMap.put(key, value);
    //return this;
  }

  @Override public TFloatVector set(int key, float value) {
    set((long) key, value);
    return this;
  }

  @Override
  public TFloatVector plusBy(int index, float delta) {
    return null;
  }


  @Override public TFloatVector times(float x) {
    SparseLongKeyFloatVector vector = new SparseLongKeyFloatVector(dim, indexToValueMap.size());
    ObjectIterator<Long2FloatMap.Entry> iter =
      indexToValueMap.long2FloatEntrySet().fastIterator();
    Long2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      vector.set(entry.getLongKey(), entry.getFloatValue() * x);
    }
    return vector;
  }

  @Override public TFloatVector timesBy(float x) {
    ObjectIterator<Long2FloatMap.Entry> iter =
      indexToValueMap.long2FloatEntrySet().fastIterator();
    Long2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      entry.setValue(entry.getFloatValue() * x);
    }
    return this;
  }

  @Override
  public TVector plus(TAbstractVector other, float x) {
    return null;
  }

  @Override
  public TFloatVector plusBy(TAbstractVector other, float x) {
    return null;
  }

  @Override public TFloatVector filter(float x) {
    SparseLongKeyFloatVector vector = new SparseLongKeyFloatVector(this.dim);

    ObjectIterator<Long2FloatMap.Entry> iter =
      indexToValueMap.long2FloatEntrySet().fastIterator();
    Long2FloatMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      float value = entry.getFloatValue();
      if (Math.abs(value) > x) {
        vector.set(entry.getLongKey(), value);
      }
    }
    vector.setRowId(rowId).setMatrixId(matrixId).setClock(clock);
    return vector;
  }

  @Override public SparseLongKeyFloatVector clone() {
    return new SparseLongKeyFloatVector(this);
  }

  @Override public void clear() {
    indexToValueMap.clear();
  }

  @Override public long nonZeroNumber() {
    long counter = 0L;
    ObjectIterator<Long2FloatMap.Entry> iter =
      indexToValueMap.long2FloatEntrySet().fastIterator();
    while (iter.hasNext()) {
      if(iter.next().getFloatValue() > 0)
        counter++;
    }

    return counter;
  }

  @Override public void clone(TVector vector) {
    assert vector instanceof SparseLongKeyFloatVector;

    this.matrixId = ((SparseLongKeyFloatVector)vector).matrixId;
    this.rowId = ((SparseLongKeyFloatVector)vector).rowId;
    this.clock = ((SparseLongKeyFloatVector)vector).clock;
    this.indexToValueMap.clear();
    this.indexToValueMap.putAll(((SparseLongKeyFloatVector)vector).indexToValueMap);
  }

  @Override public double squaredNorm() {
    ObjectIterator<Long2FloatMap.Entry> iter = indexToValueMap.long2FloatEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      double v = iter.next().getFloatValue();
      sum += v * v;
    }
    return sum;
  }

  public double norm() {
    ObjectIterator<Long2FloatMap.Entry> iter = indexToValueMap.long2FloatEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      sum += Math.abs(iter.next().getFloatValue());
    }
    return sum;
  }

  @Override public double sparsity() {
    return nonZeroNumber() / dim;
  }

  @Override public RowType getType() {
    return RowType.T_FLOAT_SPARSE_LONGKEY;
  }

  @Override public int size() {
    return indexToValueMap.size();
  }

  public double sum() {
    ObjectIterator<Long2FloatMap.Entry> iter = indexToValueMap.long2FloatEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      double v = iter.next().getFloatValue();
      sum += v;
    }
    return sum;
  }

  public Long2FloatOpenHashMap getIndexToValueMap() {
    return indexToValueMap;
  }

  @Override
  public TLongFloatVector elemUpdate(LongFloatElemUpdater updater, ElemUpdateParam param) {
    ObjectIterator<Long2FloatMap.Entry> iter = indexToValueMap.long2FloatEntrySet().fastIterator();
    Long2FloatMap.Entry entry;
    while (iter.hasNext()) {
      entry = iter.next();
      entry.setValue(updater.action(entry.getLongKey(), entry.getFloatValue(), param));
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
    Long2FloatOpenHashMap data = new Long2FloatOpenHashMap(dim);
    IntStream.range(0,length).forEach(i->data.put(buf.readLong(), buf.readFloat()));
    this.dim = dim;
    this.indexToValueMap = data;

  }

  @Override
  public int bufferLen() {
    return 4 + (8 + 8) * indexToValueMap.size();
  }
}
