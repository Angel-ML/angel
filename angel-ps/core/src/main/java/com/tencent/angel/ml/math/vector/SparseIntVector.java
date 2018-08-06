/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
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
import it.unimi.dsi.fastutil.ints.Int2FloatOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.stream.IntStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sparse int vector.
 */
public class SparseIntVector extends TIntVector implements Serialize{

  private final static Log LOG = LogFactory.getLog(SparseIntVector.class);

  /**
   * the init size
   */
  private final static int INIT_SIZE = 64;

  /**
   * store the data
   */
  Int2IntOpenHashMap hashMap;

  /**
   * init the vector by init size
   * 
   */
  public SparseIntVector() {
    super();
    this.hashMap = new Int2IntOpenHashMap(INIT_SIZE);
  }

  /**
   * init the vector by setting the dim
   * 
   * @param dim
   */
  public SparseIntVector(int dim) {
    this(dim, INIT_SIZE);
  }

  /**
   * init the vector by setting the dim and capacity
   * 
   * @param dim
   * @param capacity
   */
  public SparseIntVector(int dim, int capacity) {
    super();
    this.dim = dim;
    this.hashMap = new Int2IntOpenHashMap(capacity);
  }

  /**
   * init the vector by setting the dim ,index and value
   * 
   * @param dim
   * @param indices
   * @param values
   */
  public SparseIntVector(int dim, int[] indices, int[] values) {
    super();
    assert indices.length == values.length;
    this.dim = dim;
    this.hashMap = new Int2IntOpenHashMap(indices, values);
  }

  /**
   * init the vector by setting the map
   * 
   * @param dim
   * @param map
   */
  public SparseIntVector(int dim, Int2IntOpenHashMap map) {
    super();
    this.dim = dim;
    this.hashMap = map;
  }

  /**
   * init the vector by another one
   * 
   * @param other
   */
  public SparseIntVector(SparseIntVector other) {
    super(other);
    this.hashMap = new Int2IntOpenHashMap(other.hashMap);
  }

  @Override
  public int get(int index) {
    return hashMap.get(index);
  }

  @Override
  public TIntVector set(int index, int value) {
    hashMap.put(index, value);
    return this;
  }

  @Override
  public TIntVector plusBy(int index, int value) {
    hashMap.addTo(index, value);
    return this;
  }

  @Override
  public int[] getIndices() {
    return hashMap.keySet().toIntArray();
  }

  @Override public TIntVector filter(int x) {
    SparseIntVector vector = new SparseIntVector(dim);
    vector.setMatrixId(matrixId).setRowId(rowId).setClock(clock);
    ObjectIterator<Int2IntMap.Entry> iter = hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      int value = entry.getIntValue();
      if (Math.abs(value) > x) {
        vector.set(entry.getIntKey(), value);
      }
    }
    return vector;
  }

  @Override public TIntVector times(int x) {
    SparseIntVector vector = new SparseIntVector(this);
    ObjectIterator<Int2IntMap.Entry> iter = vector.hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while(iter.hasNext()) {
      entry.setValue(entry.getIntValue() * x);
    }
    return vector;
  }

  @Override public TIntVector timesBy(int x) {
    ObjectIterator<Int2IntMap.Entry> iter = hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while(iter.hasNext()) {
      entry = iter.next();
      entry.setValue(entry.getIntValue() * x);
    }
    return this;
  }

  @Override
  public int[] getValues() {
    return hashMap.values().toIntArray();
  }

  @Override
  public TVector plusBy(TAbstractVector other) {
    assert dim == other.getDimension();
    if(other instanceof SparseIntVector)
      return plusBy((SparseIntVector) other);
    if(other instanceof DenseIntVector)
      return plusBy((DenseIntVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  private SparseIntVector plusBy(SparseIntVector other) {
    ObjectIterator<Int2IntMap.Entry> iter = other.hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.addTo(entry.getIntKey(), entry.getIntValue());
    }
    return this;
  }

  private SparseIntVector plusBy(DenseIntVector other) {
    for(int i = 0; i < dim; i++) {
      if(other.values[i] != 0) {
        this.hashMap.addTo(i, other.values[i]);
      }
    }
    return this;
  }

  @Override
  public TIntVector plusBy(TAbstractVector other, int x) {
    assert dim == other.getDimension();
    if(other instanceof SparseIntVector)
      return plusBy((SparseIntVector) other, x);
    if(other instanceof DenseIntVector)
      return plusBy((DenseIntVector) other, x);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plusBy " + other.getClass().getName());
  }

  @Override public long sum() {
    ObjectIterator<Int2IntMap.Entry> iter = hashMap.int2IntEntrySet().iterator();
    long sum = 0;
    while (iter.hasNext()) {
      double v = iter.next().getIntValue();
      sum += v;
    }
    return sum;
  }

  private SparseIntVector plusBy(SparseIntVector other, int x) {
    ObjectIterator<Int2IntMap.Entry> iter = other.hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      this.hashMap.addTo(entry.getIntKey(), entry.getIntValue() * x);
    }
    return this;
  }

  private SparseIntVector plusBy(DenseIntVector other, int x) {
    for(int i = 0; i < dim; i++) {
      if(other.values[i] != 0) {
        this.hashMap.addTo(i, other.values[i] * x);
      }
    }
    return this;
  }

  @Override
  public TVector plus(TAbstractVector other) {
    assert dim == other.getDimension();
    if(other instanceof SparseIntVector) {
      return plus((SparseIntVector) other);
    }

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private SparseIntVector plus(SparseIntVector other) {
    SparseIntVector baseVector = null;
    SparseIntVector streamVector = null;
    if(this.size() >= other.size()) {
      baseVector = new SparseIntVector(this);
      streamVector = other;
    } else {
      baseVector = new SparseIntVector(other);
      streamVector = this;
    }

    return baseVector.plusBy(streamVector);
  }

  @Override
  public TIntVector plus(TAbstractVector other, int x) {
    assert dim == other.getDimension();
    if(other instanceof SparseIntVector) {
      return plus((SparseIntVector) other, x);
    }

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " plus " + other.getClass().getName());
  }

  private SparseIntVector plus(SparseIntVector other, int x) {
    SparseIntVector baseVector = null;
    SparseIntVector streamVector = null;
    if(this.size() >= other.size()) {
      baseVector = new SparseIntVector(this);
      streamVector = other;
      streamVector.timesBy(x);
    } else {
      baseVector = new SparseIntVector(other);
      baseVector.timesBy(x);
      streamVector = this;
    }

    return baseVector.plusBy(streamVector);
  }

  @Override
  public double dot(TAbstractVector other) {
    assert dim == other.getDimension();
    if(other instanceof SparseIntVector)
      return dot((SparseIntVector) other);
    if(other instanceof DenseIntVector)
      return dot((DenseIntVector) other);

    throw new UnsupportedOperationException("Unsupportted operation: "
      + this.getClass().getName() + " dot " + other.getClass().getName());
  }

  private double dot(SparseIntVector other) {
    double ret = 0;
    SparseIntVector baseVector = null;
    SparseIntVector streamVector = null;
    if(this.size() >= other.size()) {
      baseVector = new SparseIntVector(this);
      streamVector = other;
    } else {
      baseVector = new SparseIntVector(other);
      streamVector = this;
    }

    ObjectIterator<Int2IntMap.Entry> iter = streamVector.hashMap.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry entry = null;
    while(iter.hasNext()) {
      entry = iter.next();
      if(baseVector.hashMap.containsKey(entry.getIntKey())) {
        ret += baseVector.hashMap.get(entry.getIntKey()) * entry.getIntValue();
      }
    }

    return ret;
  }

  private double dot(DenseIntVector other) {
    return other.dot(this);
  }

  @Override
  public SparseIntVector clone() {
    return new SparseIntVector(this);
  }

  @Override
  public void clear() {
    if (hashMap != null) {
      hashMap.clear();
    }
  }

  @Override
  public long nonZeroNumber() {
    long ret = 0;
    if (hashMap != null) {
      ObjectIterator<Int2IntMap.Entry> iter = this.hashMap.int2IntEntrySet().fastIterator();
      while (iter.hasNext()) {
        if (iter.next().getIntValue() != 0) {
          ret++;
        }
      }
    }

    return ret;
  }

  @Override
  public void clone(TVector row) {
    super.clone(row);
    hashMap.clear();
    hashMap.putAll(((SparseIntVector) row).hashMap);
  }

  @Override public double squaredNorm() {
    ObjectIterator<Int2IntMap.Entry> iter = hashMap.int2IntEntrySet().iterator();
    double sum = 0;
    while (iter.hasNext()) {
      double v = iter.next().getIntValue();
      sum += v * v;
    }
    return sum;
  }

  @Override
  public double sparsity() {
    return (double)nonZeroNumber() / dim;
  }

  @Override
  public RowType getType() {
    return RowType.T_INT_SPARSE;
  }

  @Override
  public int size() {
    return hashMap.size();
  }

  public Int2IntOpenHashMap getIndexToValueMap() {
    return hashMap;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(dim);
    buf.writeInt(hashMap.size());
    hashMap.forEach((key, value) -> {
      buf.writeInt(key);
      buf.writeFloat(value);
    });
  }

  @Override
  public void deserialize(ByteBuf buf) {
    int dim = buf.readInt();
    int length = buf.readInt();
    Int2IntOpenHashMap data = new Int2IntOpenHashMap(length);
    IntStream.range(0,length).forEach(i-> data.put(buf.readInt(), buf.readInt()));
    this.dim = dim;
    this.hashMap = data;
  }

  @Override
  public int bufferLen() {
    return 4 + (4 + 4) * hashMap.size();
  }
}
