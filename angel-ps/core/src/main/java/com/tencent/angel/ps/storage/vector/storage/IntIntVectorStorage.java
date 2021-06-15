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
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.IntElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * A int key double value storage: use a IntIntVector as storage
 */
public class IntIntVectorStorage extends IntIntStorage {

  /**
   * A vector storage: it can use DENSE,SPARSE and SORTED storage type
   */
  private IntIntVector vector;

  public IntIntVectorStorage(IntIntVector vector, long indexOffset) {
    super(indexOffset);
    this.vector = vector;
  }

  public IntIntVectorStorage() {
    this(null, 0L);
  }

  public IntIntVector getVector() {
    return vector;
  }

  public void setVector(IntIntVector vector) {
    this.vector = vector;
  }

  @Override
  public void indexGet(KeyType keyType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    if (keyType != KeyType.INT) {
      throw new UnsupportedOperationException(
          this.getClass().getName() + " only support int type index now");
    }

    if (func != null) {
      for (int i = 0; i < indexSize; i++) {
        out.writeInt(initAndGet(in.readInt(), func));
      }
    } else {
      for (int i = 0; i < indexSize; i++) {
        out.writeInt(get(in.readInt()));
      }
    }
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    switch (updateType) {
      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        updateUseSparse(buf, op);
        break;

      case T_INT_DENSE:
      case T_INT_DENSE_COMPONENT:
        updateUseDense(buf, op);
        break;

      default: {
        throw new UnsupportedOperationException(
            "Unsupport operation: update " + updateType + " to " + this.getClass().getName());
      }
    }
  }

  private void updateUseDense(ByteBuf buf, UpdateOp op) {
    int size = ByteBufSerdeUtils.deserializeInt(buf);
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        int actualIndex = i + (int) indexOffset;
        int oldValue = get(actualIndex);
        set(actualIndex, oldValue + ByteBufSerdeUtils.deserializeInt(buf));
      }
    } else {
      for (int i = 0; i < size; i++) {
        set(i + (int) indexOffset, ByteBufSerdeUtils.deserializeInt(buf));
      }
    }
  }

  private void updateUseSparse(ByteBuf buf, UpdateOp op) {
    int size = ByteBufSerdeUtils.deserializeInt(buf);
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        int index = ByteBufSerdeUtils.deserializeInt(buf);
        int oldValue = get(index);
        set(index, oldValue + ByteBufSerdeUtils.deserializeInt(buf));
      }
    } else {
      for (int i = 0; i < size; i++) {
        set(ByteBufSerdeUtils.deserializeInt(buf), ByteBufSerdeUtils.deserializeInt(buf));
      }
    }
  }

  @Override
  public void elemUpdate(IntElemUpdateFunc func) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case DENSE: {
        int[] values = getVector().getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          values[i] = func.update();
        }
        break;
      }

      case SPARSE: {
        // Just update the exist element now!!
        ObjectIterator<Entry> iter = getVector().getStorage().entryIterator();
        Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          entry.setValue(func.update());
        }
        break;
      }

      case SORTED: {
        // Just update the exist element now!!
        int[] values = getVector().getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          values[i] = func.update();
        }
        break;
      }

      default:
        throw new UnsupportedOperationException("Unsupport storage method " + method);
    }
  }

  @Override
  public void clear() {
    VectorStorageUtils.clear(vector);
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
  public IntIntVectorStorage adaptiveClone() {
    if(isSparse()) {
      return new IntIntVectorStorage(VFactory
          .sortedIntVector(vector.getDim(), vector.getStorage().getIndices(),
              vector.getStorage().getValues()), indexOffset);
    } else {
      return this;
    }
  }

  @Override
  public int get(int index) {
    return vector.get(index - (int) indexOffset);
  }

  @Override
  public void set(int index, int value) {
    vector.set(index - (int) indexOffset, value);
  }

  @Override
  public int[] get(int[] indices) {
    int[] values = new int[indices.length];
    for (int i = 0; i < indices.length; i++) {
      values[i] = get(indices[i]);
    }
    return values;
  }

  @Override
  public void set(int[] indices, int[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], values[i]);
    }
  }

  @Override
  public void addTo(int index, int value) {
    set(index, get(index) + value);
  }

  @Override
  public void addTo(int[] indices, int[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      addTo(indices[i], values[i]);
    }
  }

  @Override
  public void mergeTo(IntIntVector mergedRow) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case DENSE: {
        int[] values = getVector().getStorage().getValues();
        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + (int) indexOffset, values[i]);
        }
        break;
      }

      case SPARSE: {
        ObjectIterator<Entry> iter = getVector().getStorage().entryIterator();
        Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          mergedRow.set(entry.getIntKey() + (int) indexOffset, entry.getIntValue());
        }

        break;
      }

      case SORTED: {
        int[] indices = getVector().getStorage().getIndices();
        int[] values = getVector().getStorage().getValues();
        for (int i = 0; i < indices.length; i++) {
          mergedRow.set(indices[i] + (int) indexOffset, values[i]);
        }

        break;
      }

      default:
        throw new UnsupportedOperationException("Unsupport storage method " + method);
    }
  }

  @Override
  public int initAndGet(int index, InitFunc func) {
    if (exist(index)) {
      return get(index);
    } else {
      int value = (int) func.action();
      set(index, value);
      return value;
    }
  }

  @Override
  public boolean exist(int index) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    if (method == StorageMethod.DENSE) {
      // TODO: just check the value is 0 or not now
      return getVector().get(index - (int) indexOffset) != 0;
    } else {
      return getVector().getStorage().hasKey(index - (int) indexOffset);
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
    vector = (IntIntVector) VectorStorageUtils.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + VectorStorageUtils.bufferLen(vector);
  }

  @Override
  public IntIntVectorStorage deepClone() {
    return new IntIntVectorStorage(vector.copy(), indexOffset);
  }
}
