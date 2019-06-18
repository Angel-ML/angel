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
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.IntElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
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
  private IntVector vector;

  public LongIntVectorStorage(IntVector vector, long indexOffset) {
    super(indexOffset);
    this.vector = vector;
  }

  public LongIntVectorStorage() {
    this(null, 0L);
  }

  public IntVector getVector() {
    return vector;
  }

  public void setVector(IntVector vector) {
    this.vector = vector;
  }

  private LongIntVector getLongIntVector() {
    return (LongIntVector) vector;
  }

  private IntIntVector getIntIntVector() {
    return (IntIntVector) vector;
  }

  @Override
  public int get(long index) {
    if (VectorStorageUtils.useIntKey(vector)) {
      return getIntIntVector().get((int) (index - getIndexOffset()));
    } else {
      return getLongIntVector().get(index - getIndexOffset());
    }
  }

  @Override
  public void set(long index, int value) {
    if (VectorStorageUtils.useIntKey(vector)) {
      getIntIntVector().set((int) (index - getIndexOffset()), value);
    } else {
      getLongIntVector().set(index - getIndexOffset(), value);
    }
  }

  @Override
  public int[] get(long[] indices) {
    int[] values = new int[indices.length];
    if (VectorStorageUtils.useIntKey(vector)) {
      for (int i = 0; i < indices.length; i++) {
        values[i] = getIntIntVector().get((int) (indices[i] - getIndexOffset()));
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        values[i] = getLongIntVector().get(indices[i] - getIndexOffset());
      }
    }

    return values;
  }

  @Override
  public void set(long[] indices, int[] values) {
    assert indices.length == values.length;
    if (VectorStorageUtils.useIntKey(vector)) {
      for (int i = 0; i < indices.length; i++) {
        getIntIntVector().set((int) (indices[i] - getIndexOffset()), values[i]);
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        getLongIntVector().set(indices[i] - getIndexOffset(), values[i]);
      }
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
      case DENSE: {
        int[] values;
        if (VectorStorageUtils.useIntKey(vector)) {
          values = getIntIntVector().getStorage().getValues();
        } else {
          values = getLongIntVector().getStorage().getValues();
        }

        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + indexOffset, values[i]);
        }
      }
      break;

      case SPARSE: {
        if (VectorStorageUtils.useIntKey(vector)) {
          ObjectIterator<Entry> iter = getIntIntVector().getStorage().entryIterator();
          Int2IntMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getIntKey() + indexOffset, entry.getIntValue());
          }
        } else {
          ObjectIterator<Long2IntMap.Entry> iter =
              getLongIntVector().getStorage().entryIterator();
          Long2IntMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getLongKey() + indexOffset, entry.getIntValue());
          }
        }

        break;
      }

      case SORTED: {
        if (VectorStorageUtils.useIntKey(vector)) {
          int[] indices = getIntIntVector().getStorage().getIndices();
          int[] values = getIntIntVector().getStorage().getValues();
          for (int i = 0; i < indices.length; i++) {
            mergedRow.set(indices[i] + indexOffset, values[i]);
          }
        } else {
          long[] indices = getLongIntVector().getStorage().getIndices();
          int[] values = getIntIntVector().getStorage().getValues();
          for (int i = 0; i < indices.length; i++) {
            mergedRow.set(indices[i] + indexOffset, values[i]);
          }
        }
      }
      break;

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
      case DENSE:
      case SORTED: {
        // Attention: Only update the exist values for sorted storage method
        int[] values;
        if (VectorStorageUtils.useIntKey(vector)) {
          values = getIntIntVector().getStorage().getValues();
        } else {
          values = getLongIntVector().getStorage().getValues();
        }
        for (int i = 0; i < values.length; i++) {
          values[i] = func.update();
        }
      }
      break;

      case SPARSE: {
        // Attention: Only update exist element
        if (VectorStorageUtils.useIntKey(vector)) {
          ObjectIterator<Int2IntMap.Entry> iter = getIntIntVector().getStorage().entryIterator();
          Int2IntMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            entry.setValue(func.update());
          }
        } else {
          ObjectIterator<Long2IntMap.Entry> iter = getLongIntVector().getStorage().entryIterator();
          Long2IntMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            entry.setValue(func.update());
          }
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
    if (method == StorageMethod.DENSE) {
      // TODO: just check the value is zero or not now
      if (VectorStorageUtils.useIntKey(vector)) {
        return getIntIntVector().getStorage().get((int) (index - indexOffset)) == 0;
      } else {
        return getLongIntVector().getStorage().get(index - indexOffset) == 0;
      }
    } else {
      // SPARSE and SORT, check index exist or not, When using SORT mode storage, the search efficiency is very low.
      if (VectorStorageUtils.useIntKey(vector)) {
        return getIntIntVector().getStorage().hasKey((int) (index - indexOffset));
      } else {
        return getLongIntVector().getStorage().hasKey(index - indexOffset);
      }
    }
  }


  @Override
  public void clear() {
    VectorStorageUtils.clear(vector);
  }

  @Override
  public LongIntVectorStorage deepClone() {
    return new LongIntVectorStorage((IntVector) vector.copy(), indexOffset);
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
      if (VectorStorageUtils.useIntKey(vector)) {
        return new LongIntVectorStorage(VFactory.sortedIntVector((int) vector.dim(),
            ((IntIntVector) vector).getStorage().getIndices(),
            ((IntIntVector) vector).getStorage().getValues()), indexOffset);
      } else {
        return new LongIntVectorStorage(VFactory.sortedLongKeyIntVector(vector.dim(),
            ((LongIntVector) vector).getStorage().getIndices(),
            ((LongIntVector) vector).getStorage().getValues()), indexOffset);
      }
    } else {
      return this;
    }
  }

  @Override
  public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    if (func != null) {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeInt(initAndGet(in.readInt(), func));
        }
      } else {
        for (int i = 0; i < indexSize; i++) {
          out.writeInt(initAndGet(in.readLong(), func));
        }
      }
    } else {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeInt(get(in.readInt()));
        }
      } else {
        for (int i = 0; i < indexSize; i++) {
          out.writeInt(get(in.readLong()));
        }
      }
    }
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    switch (updateType) {
      case T_INT_SPARSE_LONGKEY:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
        updateUseLongIntSparse(buf, op);
        break;

      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        updateUseIntIntSparse(buf, op);
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

  private void updateUseLongIntSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntIntVector().set(index, getIntIntVector().get(index) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongIntVector().set(index, getLongIntVector().get(index) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntIntVector().set((int) buf.readLong(), buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongIntVector().set(buf.readLong(), buf.readInt());
        }
      }
    }
  }

  private void updateUseIntIntSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = buf.readInt();
          getIntIntVector().set(index, getIntIntVector().get(index) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongIntVector().set(index, getLongIntVector().get(index) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntIntVector().set(buf.readInt(), buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongIntVector().set(buf.readInt(), buf.readInt());
        }
      }
    }
  }

  private void updateUseIntIntDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntIntVector().set(i, getIntIntVector().get(i) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongIntVector().set(i, getLongIntVector().get(i) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntIntVector().set(i, buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongIntVector().set(i, buf.readInt());
        }
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
    vector = (IntVector) VectorStorageUtils.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + VectorStorageUtils.bufferLen(vector);
  }
}
