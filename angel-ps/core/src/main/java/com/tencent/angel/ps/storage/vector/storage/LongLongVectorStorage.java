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
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.vector.LongVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.LongElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * A long key long value storage: use a LongVector as storage
 */
public class LongLongVectorStorage extends LongLongStorage {

  /**
   * A vector storage: it can be LongLongVector or IntLongVector and can use DENSE,SPARSE and SORTED
   * storage type
   */
  private LongVector vector;

  public LongLongVectorStorage(LongVector vector, long indexOffset) {
    super(indexOffset);
    this.vector = vector;
  }

  public LongLongVectorStorage() {
    this(null, 0L);
  }

  public LongVector getVector() {
    return vector;
  }

  public void setVector(LongVector vector) {
    this.vector = vector;
  }

  private LongLongVector getLongLongVector() {
    return (LongLongVector) vector;
  }

  private IntLongVector getIntLongVector() {
    return (IntLongVector) vector;
  }

  @Override
  public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    if (func != null) {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeLong(initAndGet(in.readInt(), func));
        }
      } else {
        for (int i = 0; i < indexSize; i++) {
          out.writeLong(initAndGet(in.readLong(), func));
        }
      }
    } else {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeLong(get(in.readInt()));
        }
      } else {
        for (int i = 0; i < indexSize; i++) {
          out.writeLong(get(in.readLong()));
        }
      }
    }
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    switch (updateType) {
      case T_LONG_SPARSE_LONGKEY:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        updateUseLongLongSparse(buf, op);
        break;

      case T_INT_SPARSE_LONGKEY:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
        updateUseLongIntSparse(buf, op);
        break;

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


  private void updateUseLongLongSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntLongVector().set(index, getIntLongVector().get(index) + buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongLongVector().set(index, getLongLongVector().get(index) + buf.readLong());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntLongVector().set(index, buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongLongVector().set(index, buf.readLong());
        }
      }
    }
  }

  private void updateUseLongIntSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntLongVector().set(index, getIntLongVector().get(index) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongLongVector().set(index, getLongLongVector().get(index) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntLongVector().set(index, buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongLongVector().set(index, buf.readInt());
        }
      }
    }
  }

  private void updateUseIntLongSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = buf.readInt();
          getIntLongVector().set(index, getIntLongVector().get(index) + buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongLongVector().set(index, getLongLongVector().get(index) + buf.readLong());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntLongVector().set(buf.readInt(), buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongLongVector().set(buf.readInt(), buf.readLong());
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
          getIntLongVector().set(index, getIntLongVector().get(index) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongLongVector().set(index, getLongLongVector().get(index) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntLongVector().set(buf.readInt(), buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongLongVector().set(buf.readInt(), buf.readInt());
        }
      }
    }
  }

  private void updateUseIntLongDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntLongVector().set(i, getIntLongVector().get(i) + buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongLongVector().set(i, getLongLongVector().get(i) + buf.readLong());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntLongVector().set(i, buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongLongVector().set(i, buf.readLong());
        }
      }
    }
  }

  private void updateUseIntIntDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntLongVector().set(i, getIntLongVector().get(i) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongLongVector().set(i, getLongLongVector().get(i) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntLongVector().set(i, buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongLongVector().set(i, buf.readInt());
        }
      }
    }
  }

  @Override
  public long get(long index) {
    if (VectorStorageUtils.useIntKey(vector)) {
      return getIntLongVector().get((int) (index - indexOffset));
    } else {
      return getLongLongVector().get(index - getIndexOffset());
    }
  }

  @Override
  public void set(long index, long value) {
    if (VectorStorageUtils.useIntKey(vector)) {
      getIntLongVector().set((int) (index - indexOffset), value);
    } else {
      getLongLongVector().set(index - indexOffset, value);
    }
  }

  @Override
  public long[] get(long[] indices) {
    long[] values = new long[indices.length];
    if (VectorStorageUtils.useIntKey(vector)) {
      for (int i = 0; i < indices.length; i++) {
        values[i] = getIntLongVector().get((int) (indices[i] - indexOffset));
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        values[i] = getLongLongVector().get(indices[i] - indexOffset);
      }
    }

    return values;
  }

  @Override
  public void set(long[] indices, long[] values) {
    assert indices.length == values.length;
    if (VectorStorageUtils.useIntKey(vector)) {
      for (int i = 0; i < indices.length; i++) {
        getIntLongVector().set((int) (indices[i] - indexOffset), values[i]);
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        getLongLongVector().set(indices[i] - indexOffset, values[i]);
      }
    }
  }

  @Override
  public void addTo(long index, long value) {
    set(index, get(index) + value);
  }

  @Override
  public void addTo(long[] indices, long[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], get(indices[i]) + values[i]);
    }
  }

  @Override
  public void mergeTo(LongLongVector mergedRow) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case DENSE: {
        long[] values;
        if (VectorStorageUtils.useIntKey(vector)) {
          values = getIntLongVector().getStorage().getValues();
        } else {
          values = getLongLongVector().getStorage().getValues();
        }

        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + indexOffset, values[i]);
        }
      }
      break;

      case SPARSE: {
        if (VectorStorageUtils.useIntKey(vector)) {
          ObjectIterator<Int2LongMap.Entry> iter = getIntLongVector().getStorage().entryIterator();
          Int2LongMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getIntKey() + indexOffset, entry.getLongValue());
          }
        } else {
          ObjectIterator<Long2LongMap.Entry> iter =
              getLongLongVector().getStorage().entryIterator();
          Long2LongMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getLongKey() + indexOffset, entry.getLongValue());
          }
        }

        break;
      }

      case SORTED: {
        if (VectorStorageUtils.useIntKey(vector)) {
          int[] indices = getIntLongVector().getStorage().getIndices();
          long[] values = getIntLongVector().getStorage().getValues();
          for (int i = 0; i < indices.length; i++) {
            mergedRow.set(indices[i] + indexOffset, values[i]);
          }
        } else {
          long[] indices = getLongLongVector().getStorage().getIndices();
          long[] values = getLongLongVector().getStorage().getValues();
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
  public long initAndGet(long index, InitFunc func) {
    if (exist(index)) {
      return get(index);
    } else {
      long value = (long) func.action();
      set(index, value);
      return value;
    }
  }

  @Override
  public boolean exist(long index) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    if (method == StorageMethod.DENSE) {
      // TODO: just check the value is zero or not now
      if (VectorStorageUtils.useIntKey(vector)) {
        return getIntLongVector().getStorage().get((int) (index - indexOffset)) == 0;
      } else {
        return getLongLongVector().getStorage().get(index - indexOffset) == 0;
      }
    } else {
      // SPARSE and SORT, check index exist or not, When using SORT mode storage, the search efficiency is very low.
      if (VectorStorageUtils.useIntKey(vector)) {
        return getIntLongVector().getStorage().hasKey((int) (index - indexOffset));
      } else {
        return getLongLongVector().getStorage().hasKey(index - indexOffset);
      }
    }
  }

  @Override
  public void elemUpdate(LongElemUpdateFunc func) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case DENSE:
      case SORTED: {
        // Attention: Only update the exist values for sorted storage method
        long[] values;
        if (VectorStorageUtils.useIntKey(vector)) {
          values = getIntLongVector().getStorage().getValues();
        } else {
          values = getLongLongVector().getStorage().getValues();
        }
        for (int i = 0; i < values.length; i++) {
          values[i] = func.update();
        }
      }
      break;

      case SPARSE: {
        // Attention: Only update exist element
        if (VectorStorageUtils.useIntKey(vector)) {
          ObjectIterator<Int2LongMap.Entry> iter = getIntLongVector().getStorage().entryIterator();
          Int2LongMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            entry.setValue(func.update());
          }
        } else {
          ObjectIterator<Long2LongMap.Entry> iter = getLongLongVector().getStorage()
              .entryIterator();
          Long2LongMap.Entry entry;
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
  public void clear() {
    VectorStorageUtils.clear(vector);
  }

  @Override
  public LongLongVectorStorage deepClone() {
    return new LongLongVectorStorage((LongVector) vector.copy(), indexOffset);
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
  public LongLongVectorStorage adaptiveClone() {
    if (isSparse()) {
      if (VectorStorageUtils.useIntKey(vector)) {
        return new LongLongVectorStorage(VFactory.sortedLongVector((int) vector.dim(),
            ((IntLongVector) vector).getStorage().getIndices(),
            ((IntLongVector) vector).getStorage().getValues()), indexOffset);
      } else {
        return new LongLongVectorStorage(VFactory.sortedLongKeyLongVector(vector.dim(),
            ((LongLongVector) vector).getStorage().getIndices(),
            ((LongLongVector) vector).getStorage().getValues()), indexOffset);
      }
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
    vector = (LongVector) VectorStorageUtils.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + VectorStorageUtils.bufferLen(vector);
  }
}
