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
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.IntElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * A long key int value storage: use a IntVector as storage
 */
public class LongIntVectorStorage extends LongIntStorage {

  /**
   * A vector storage: it can be IntIntVector or LongIntVector and can use DENSE,SPARSE and SORTED
   * storage type
   */
  private LongIntVector vector;

  public LongIntVectorStorage(LongIntVector vector, long indexOffset) {
    super(indexOffset);
    this.vector = vector;
  }

  public LongIntVectorStorage() {
    this(null, 0L);
  }

  public LongIntVector getVector() {
    return vector;
  }

  public void setVector(LongIntVector vector) {
    this.vector = vector;
  }


  @Override
  public int get(long index) {
    return vector.get(index - indexOffset);
  }

  @Override
  public void set(long index, int value) {
    vector.set(index - indexOffset, value);
  }

  @Override
  public int[] get(long[] indices) {
    int[] values = new int[indices.length];
    for (int i = 0; i < indices.length; i++) {
      values[i] = get(indices[i]);
    }

    return values;
  }

  @Override
  public void set(long[] indices, int[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], values[i]);
    }
  }

  @Override
  public void addTo(long index, int value) {
    set(index, get(index) + value);
  }

  @Override
  public void addTo(long[] indices, int[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], get(indices[i]) + values[i]);
    }
  }

  @Override
  public void mergeTo(LongIntVector mergedRow) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case SPARSE: {
        ObjectIterator<Long2IntMap.Entry> iter = vector.getStorage().entryIterator();
        Long2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          mergedRow.set(entry.getLongKey() + indexOffset, entry.getIntValue());
        }
        break;
      }

      default:
        throw new UnsupportedOperationException("unsupport storage method " + method);
    }
  }

  @Override
  public int initAndGet(long index, InitFunc func) {
    if (exist(index)) {
      return get(index);
    } else {
      int value = (int) func.action();
      set(index, value);
      return value;
    }
  }

  @Override
  public void elemUpdate(IntElemUpdateFunc func) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case SPARSE: {
        // Attention: Only update exist element
        ObjectIterator<Long2IntMap.Entry> iter = vector.getStorage().entryIterator();
        Long2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          entry.setValue(func.update());
        }
        break;
      }

      default:
        throw new UnsupportedOperationException("unsupport storage method " + method);
    }
  }

  @Override
  public boolean exist(long index) {
    return vector.getStorage().hasKey(index - indexOffset);
  }


  @Override
  public void clear() {
    VectorStorageUtils.clear(vector);
  }

  @Override
  public LongIntVectorStorage deepClone() {
    return new LongIntVectorStorage(vector.copy(), indexOffset);
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
  public LongIntVectorStorage adaptiveClone() {
    if (isSparse()) {
      return new LongIntVectorStorage(VFactory.sortedLongKeyIntVector(vector.dim(),
          vector.getStorage().getIndices(),
          vector.getStorage().getValues()), indexOffset);
    } else {
      return this;
    }
  }

  @Override
  public void indexGet(KeyType keyType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    if (func != null) {
      if (keyType == KeyType.INT) {
        throw new UnsupportedOperationException("Only support long index for Long key storage");
      } else {
        for (int i = 0; i < indexSize; i++) {
          ByteBufSerdeUtils.serializeInt(out, initAndGet(ByteBufSerdeUtils.deserializeLong(in), func));
        }
      }
    } else {
      if (keyType == KeyType.INT) {
        throw new UnsupportedOperationException("Only support long index for Long key storage");
      } else {
        for (int i = 0; i < indexSize; i++) {
          ByteBufSerdeUtils.serializeInt(out, get(ByteBufSerdeUtils.deserializeLong(in)));
        }
      }
    }
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    switch (updateType) {
      case T_INT_SPARSE_LONGKEY:
        updateUseLongIntSparse(buf, op);
        break;

      default: {
        throw new UnsupportedOperationException(
            "Unsupport operation: update " + updateType + " to " + this.getClass().getName());
      }
    }
  }

  private void updateUseLongIntSparse(ByteBuf buf, UpdateOp op) {
    int size = ByteBufSerdeUtils.deserializeInt(buf);
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        long index = buf.readLong();
        int oldValue = get(index);
        set(index, oldValue + ByteBufSerdeUtils.deserializeInt(buf));
      }
    } else {
      for (int i = 0; i < size; i++) {
        set(ByteBufSerdeUtils.deserializeLong(buf), ByteBufSerdeUtils.deserializeInt(buf));
      }
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
    vector = (LongIntVector) VectorStorageUtils.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + VectorStorageUtils.bufferLen(vector);
  }
}
