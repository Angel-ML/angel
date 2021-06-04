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


package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.DoubleElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * A long key double value storage: use a LongDoubleVector as storage
 */
public class LongDoubleVectorStorage extends LongDoubleStorage {

  /**
   * A vector storage
   */
  private LongDoubleVector vector;

  public LongDoubleVectorStorage(LongDoubleVector vector, long indexOffset) {
    super(indexOffset);
    this.vector = vector;
  }

  public LongDoubleVectorStorage() {
    this(null, 0L);
  }

  public void setVector(LongDoubleVector vector) {
    this.vector = vector;
  }

  public LongDoubleVector getVector() {
    return vector;
  }

  @Override
  public void indexGet(KeyType keyType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    if (func != null) {
      if (keyType == KeyType.INT) {
        throw new UnsupportedOperationException("Only support long index for Long key storage");
      } else {
        for (int i = 0; i < indexSize; i++) {
          ByteBufSerdeUtils.serializeDouble(out, initAndGet(ByteBufSerdeUtils.deserializeLong(in), func));
        }
      }
    } else {
      if (keyType == KeyType.INT) {
        throw new UnsupportedOperationException("Only support long index for Long key storage");
      } else {
        for (int i = 0; i < indexSize; i++) {
          ByteBufSerdeUtils.serializeDouble(out, get(ByteBufSerdeUtils.deserializeLong(in)));
        }
      }
    }
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    switch (updateType) {
      case T_DOUBLE_SPARSE_LONGKEY:
        updateUseLongDoubleSparse(buf, op);
        break;

      case T_FLOAT_SPARSE_LONGKEY:
        updateUseLongFloatSparse(buf, op);
        break;

      case T_LONG_SPARSE_LONGKEY:
        updateUseLongLongSparse(buf, op);
        break;

      case T_INT_SPARSE_LONGKEY:
        updateUseLongIntSparse(buf, op);
        break;

      default: {
        throw new UnsupportedOperationException(
            "Unsupport operation: update " + updateType + " to " + this.getClass().getName());
      }
    }

  }

  private void updateUseLongDoubleSparse(ByteBuf buf, UpdateOp op) {

    int size = ByteBufSerdeUtils.deserializeInt(buf);
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        long index = ByteBufSerdeUtils.deserializeLong(buf);
        double oldValue = get(index);
        set(index, oldValue + ByteBufSerdeUtils.deserializeDouble(buf));
      }
    } else {
      for (int i = 0; i < size; i++) {
        set(ByteBufSerdeUtils.deserializeLong(buf), ByteBufSerdeUtils.deserializeDouble(buf));
      }
    }
  }

  private void updateUseLongFloatSparse(ByteBuf buf, UpdateOp op) {

    int size = ByteBufSerdeUtils.deserializeInt(buf);
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        long index = ByteBufSerdeUtils.deserializeLong(buf);
        double oldValue = get(index);
        set(index, oldValue + ByteBufSerdeUtils.deserializeFloat(buf));
      }
    } else {
      for (int i = 0; i < size; i++) {
        set(ByteBufSerdeUtils.deserializeLong(buf), ByteBufSerdeUtils.deserializeFloat(buf));
      }
    }
  }

  private void updateUseLongLongSparse(ByteBuf buf, UpdateOp op) {

    int size = ByteBufSerdeUtils.deserializeInt(buf);
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        long index = ByteBufSerdeUtils.deserializeLong(buf);
        double oldValue = get(index);
        set(index, oldValue + ByteBufSerdeUtils.deserializeLong(buf));
      }
    } else {
      for (int i = 0; i < size; i++) {
        set(ByteBufSerdeUtils.deserializeLong(buf), ByteBufSerdeUtils.deserializeLong(buf));
      }
    }
  }

  private void updateUseLongIntSparse(ByteBuf buf, UpdateOp op) {

    int size = ByteBufSerdeUtils.deserializeInt(buf);
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        long index = ByteBufSerdeUtils.deserializeLong(buf);
        double oldValue = get(index);
        set(index, oldValue + ByteBufSerdeUtils.deserializeInt(buf));
      }
    } else {
      for (int i = 0; i < size; i++) {
        set(ByteBufSerdeUtils.deserializeLong(buf), ByteBufSerdeUtils.deserializeInt(buf));
      }
    }
  }

  @Override
  public double get(long index) {
    return vector.get(index - indexOffset);
  }

  @Override
  public void set(long index, double value) {
    vector.set(index - indexOffset, value);
  }

  @Override
  public double[] get(long[] indices) {
    double[] values = new double[indices.length];
    for (int i = 0; i < indices.length; i++) {
      values[i] = get(indices[i]);
    }
    return values;
  }

  @Override
  public void set(long[] indices, double[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], values[i]);
    }
  }

  @Override
  public void addTo(long index, double value) {
    set(index, get(index) + value);
  }

  @Override
  public void addTo(long[] indices, double[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], get(indices[i]) + values[i]);
    }
  }

  @Override
  public void mergeTo(LongDoubleVector mergedRow) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case SPARSE: {
        ObjectIterator<Long2DoubleMap.Entry> iter = vector.getStorage().entryIterator();
        Long2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          mergedRow.set(entry.getLongKey() + indexOffset, entry.getDoubleValue());
        }

        break;
      }

      default:
        throw new UnsupportedOperationException("unsupport storage method " + method);
    }
  }

  @Override
  public double initAndGet(long index, InitFunc func) {
    if (exist(index)) {
      return get(index);
    } else {
      float value = (float) func.action();
      set(index, value);
      return value;
    }
  }

  @Override
  public void elemUpdate(DoubleElemUpdateFunc func) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case SPARSE: {
        // Attention: Only update exist element
        ObjectIterator<Long2DoubleMap.Entry> iter = vector.getStorage()
            .entryIterator();
        Long2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          entry.setValue(func.update());
        }
      }
      break;

      default:
        throw new UnsupportedOperationException("unsupport storage method " + method);
    }
  }

  @Override
  public boolean exist(long index) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    // SPARSE and SORT, check index exist or not, When using SORT mode storage, the search efficiency is very low.
    return vector.getStorage().hasKey(index - indexOffset);
  }

  @Override
  public void clear() {
    VectorStorageUtils.clear(vector);
  }

  @Override
  public LongDoubleVectorStorage deepClone() {
    return new LongDoubleVectorStorage(vector.copy(), indexOffset);
  }

  @Override
  public int size() {
    return VectorStorageUtils.size(vector);
  }

  @Override
  public boolean isDense() {
    return VectorStorageUtils.isDense(vector);
  }

  @Override
  public boolean isSparse() {
    return VectorStorageUtils.isSparse(vector);
  }

  @Override
  public boolean isSorted() {
    return VectorStorageUtils.isSorted(vector);
  }

  @Override
  public LongDoubleVectorStorage adaptiveClone() {
    if (isSparse()) {
      return new LongDoubleVectorStorage(VFactory.sortedLongKeyDoubleVector(vector.dim(),
          vector.getStorage().getIndices(), vector.getStorage().getValues()), indexOffset);
    } else {
      return this;
    }
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    VectorStorageUtils.serialize(buf, vector);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    vector = (LongDoubleVector) VectorStorageUtils.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + VectorStorageUtils.bufferLen(vector);
  }
}
