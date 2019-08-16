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


import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.LongElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2LongMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * A int key long value storage: use a IntFloatVector as storage
 */
public class IntLongVectorStorage extends IntLongStorage {

  private IntLongVector vector;

  public IntLongVectorStorage(IntLongVector vector, long indexOffset) {
    super(indexOffset);
    this.vector = vector;
  }

  public IntLongVectorStorage() {
    this(null, 0L);
  }

  public IntLongVector getVector() {
    return vector;
  }

  public void setVector(IntLongVector vector) {
    this.vector = vector;
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    switch (updateType) {
      case T_LONG_SPARSE:
      case T_LONG_SPARSE_COMPONENT:
        updateUseIntLongSparse(buf, op);
        break;

      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        updateUseIntIntSparse(buf, op);
        break;

      case T_LONG_DENSE:
      case T_LONG_DENSE_COMPONENT:
        updateUseIntLongDense(buf, op);
        break;

      case T_INT_DENSE:
      case T_INT_DENSE_COMPONENT:
        updateUseIntIntDense(buf, op);
        break;

      default: {
        throw new UnsupportedOperationException(
            "Unsupport operation: update " + updateType + " to " + this.getClass().getName());
      }
    }

  }

  private void updateUseIntLongDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        getVector().set(i, getVector().get(i) + buf.readLong());
      }
    } else {
      for (int i = 0; i < size; i++) {
        getVector().set(i, buf.readLong());
      }
    }
  }

  private void updateUseIntIntDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        getVector().set(i, getVector().get(i) + buf.readInt());
      }
    } else {
      for (int i = 0; i < size; i++) {
        getVector().set(i, buf.readInt());
      }
    }
  }

  private void updateUseIntLongSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        int index = buf.readInt();
        getVector().set(index, getVector().get(index) + buf.readLong());
      }
    } else {
      for (int i = 0; i < size; i++) {
        getVector().set(buf.readInt(), buf.readLong());
      }
    }
  }


  private void updateUseIntIntSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        int index = buf.readInt();
        getVector().set(index, getVector().get(index) + buf.readInt());
      }
    } else {
      for (int i = 0; i < size; i++) {
        getVector().set(buf.readInt(), buf.readInt());
      }
    }
  }


  @Override
  public long get(int index) {
    return getVector().get(index - (int) indexOffset);
  }

  @Override
  public void set(int index, long value) {
    getVector().set(index - (int) indexOffset, value);
  }

  @Override
  public long[] get(int[] indices) {
    long[] values = new long[indices.length];
    for (int i = 0; i < indices.length; i++) {
      values[i] = get(indices[i]);
    }
    return values;
  }

  @Override
  public void set(int[] indices, long[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], values[i]);
    }
  }

  @Override
  public void addTo(int index, long value) {
    set(index, get(index) + value);
  }

  @Override
  public void addTo(int[] indices, long[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      addTo(indices[i], values[i]);
    }
  }

  @Override
  public void mergeTo(IntLongVector mergedRow) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case DENSE: {
        long[] values = getVector().getStorage().getValues();
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
          mergedRow.set(entry.getIntKey() + (int) indexOffset, entry.getLongValue());
        }

        break;
      }

      case SORTED: {
        int[] indices = getVector().getStorage().getIndices();
        long[] values = getVector().getStorage().getValues();
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
  public long initAndGet(int index, InitFunc func) {
    if (exist(index)) {
      return get(index);
    } else {
      long value = (long) func.action();
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
  public void elemUpdate(LongElemUpdateFunc func) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case DENSE: {
        long[] values = getVector().getStorage().getValues();
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
        long[] values = getVector().getStorage().getValues();
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
  public IntLongVectorStorage deepClone() {
    return new IntLongVectorStorage(vector.copy(), indexOffset);
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
  public IntLongVectorStorage adaptiveClone() {
    if(isSparse()) {
      return new IntLongVectorStorage(VFactory
          .sortedLongVector(vector.getDim(), vector.getStorage().getIndices(),
              vector.getStorage().getValues()), indexOffset);
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
    vector = (IntLongVector) VectorStorageUtils.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + VectorStorageUtils.bufferLen(vector);
  }

  @Override
  public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    if (indexType != IndexType.INT) {
      throw new UnsupportedOperationException(
          this.getClass().getName() + " only support int type index now");
    }

    if (func != null) {
      for (int i = 0; i < indexSize; i++) {
        out.writeLong(initAndGet(in.readInt(), func));
      }
    } else {
      for (int i = 0; i < indexSize; i++) {
        out.writeLong(get(in.readInt()));
      }
    }
  }
}
