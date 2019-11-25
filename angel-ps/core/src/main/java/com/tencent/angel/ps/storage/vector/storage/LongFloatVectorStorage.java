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
import com.tencent.angel.ml.math2.vector.FloatVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.FloatElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A long key float value storage: use a FloatVector as storage
 */
public class LongFloatVectorStorage extends LongFloatStorage {
  private final static Log LOG = LogFactory.getLog(
      LongFloatVectorStorage.class);
  /**
   * A vector storage: it can be IntFloatVector or LongFloatVector and can use DENSE,SPARSE and
   * SORTED storage type
   */
  private FloatVector vector;

  public LongFloatVectorStorage(FloatVector vector, long indexOffset) {
    super(indexOffset);
    this.vector = vector;
  }

  public LongFloatVectorStorage() {
    this(null, 0L);
  }

  public FloatVector getVector() {
    return vector;
  }

  public void setVector(FloatVector vector) {
    this.vector = vector;
  }

  private LongFloatVector getLongFloatVector() {
    return (LongFloatVector) vector;
  }

  private IntFloatVector getIntFloatVector() {
    return (IntFloatVector) vector;
  }

  @Override
  public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    if (func != null) {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeFloat(initAndGet(in.readInt(), func));
        }
      } else {
        for (int i = 0; i < indexSize; i++) {
          out.writeFloat(initAndGet(in.readLong(), func));
        }
      }
    } else {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeFloat(get(in.readInt()));
        }
      } else {
        for (int i = 0; i < indexSize; i++) {
          out.writeFloat(get(in.readLong()));
        }
      }
    }
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    long startTs = System.currentTimeMillis();
    switch (updateType) {
      case T_FLOAT_SPARSE_LONGKEY:
      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        updateUseLongFloatSparse(buf, op);
        break;

      case T_LONG_SPARSE_LONGKEY:
      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        updateUseLongLongSparse(buf, op);
        break;

      case T_INT_SPARSE_LONGKEY:
      case T_INT_SPARSE_LONGKEY_COMPONENT:
        updateUseLongIntSparse(buf, op);
        break;

      case T_FLOAT_SPARSE:
      case T_FLOAT_SPARSE_COMPONENT:
        updateUseIntFloatSparse(buf, op);
        break;

      case T_LONG_SPARSE:
      case T_LONG_SPARSE_COMPONENT:
        updateUseIntLongSparse(buf, op);
        break;

      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        updateUseIntIntSparse(buf, op);
        break;

      case T_FLOAT_DENSE:
      case T_FLOAT_DENSE_COMPONENT:
        updateUseIntFloatDense(buf, op);
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


  private void updateUseLongFloatSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntFloatVector().set(index, getIntFloatVector().get(index) + buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongFloatVector().set(index, getLongFloatVector().get(index) + buf.readFloat());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntFloatVector().set(index, buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongFloatVector().set(index, buf.readFloat());
        }
      }
    }
  }

  private void updateUseLongLongSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntFloatVector().set(index, getIntFloatVector().get(index) + buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongFloatVector().set(index, getLongFloatVector().get(index) + buf.readLong());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntFloatVector().set(index, buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongFloatVector().set(index, buf.readLong());
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
          getIntFloatVector().set(index, getIntFloatVector().get(index) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongFloatVector().set(index, getLongFloatVector().get(index) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntFloatVector().set(index, buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongFloatVector().set(index, buf.readInt());
        }
      }
    }
  }

  private void updateUseIntFloatSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = buf.readInt();
          getIntFloatVector().set(index, getIntFloatVector().get(index) + buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongFloatVector().set(index, getLongFloatVector().get(index) + buf.readFloat());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = buf.readInt();
          getIntFloatVector().set(index, buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongFloatVector().set(index, buf.readFloat());
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
          getIntFloatVector().set(index, getIntFloatVector().get(index) + buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongFloatVector().set(index, getLongFloatVector().get(index) + buf.readLong());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = buf.readInt();
          getIntFloatVector().set(index, buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongFloatVector().set(index, buf.readLong());
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
          getIntFloatVector().set(index, getIntFloatVector().get(index) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongFloatVector().set(index, getLongFloatVector().get(index) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = buf.readInt();
          getIntFloatVector().set(index, buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongFloatVector().set(index, buf.readInt());
        }
      }
    }
  }

  private void updateUseIntFloatDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntFloatVector().set(i, getIntFloatVector().get(i) + buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongFloatVector().set(i, getLongFloatVector().get(i) + buf.readFloat());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntFloatVector().set(i, buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongFloatVector().set(i, buf.readFloat());
        }
      }
    }
  }

  private void updateUseIntLongDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntFloatVector().set(i, getIntFloatVector().get(i) + buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongFloatVector().set(i, getLongFloatVector().get(i) + buf.readLong());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntFloatVector().set(i, buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongFloatVector().set(i, buf.readLong());
        }
      }
    }
  }

  private void updateUseIntIntDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntFloatVector().set(i, getIntFloatVector().get(i) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongFloatVector().set(i, getLongFloatVector().get(i) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntFloatVector().set(i, buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongFloatVector().set(i, buf.readInt());
        }
      }
    }
  }

  @Override
  public float get(long index) {
    if (VectorStorageUtils.useIntKey(vector)) {
      return getIntFloatVector().get((int) (index - indexOffset));
    } else {
      return getLongFloatVector().get(index - indexOffset);
    }
  }

  @Override
  public void set(long index, float value) {
    if (VectorStorageUtils.useIntKey(vector)) {
      getIntFloatVector().set((int) (index - indexOffset), value);
    } else {
      getLongFloatVector().set(index - indexOffset, value);
    }
  }

  @Override
  public float[] get(long[] indices) {
    float[] values = new float[indices.length];
    if (VectorStorageUtils.useIntKey(vector)) {
      for (int i = 0; i < indices.length; i++) {
        values[i] = getIntFloatVector().get((int) (indices[i] - indexOffset));
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        values[i] = getLongFloatVector().get(indices[i] - indexOffset);
      }
    }

    return values;
  }

  @Override
  public void set(long[] indices, float[] values) {
    assert indices.length == values.length;
    if (VectorStorageUtils.useIntKey(vector)) {
      for (int i = 0; i < indices.length; i++) {
        getIntFloatVector().set((int) (indices[i] - indexOffset), values[i]);
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        getLongFloatVector().set(indices[i] - indexOffset, values[i]);
      }
    }
  }

  @Override
  public void addTo(long index, float value) {
    set(index, get(index) + value);
  }

  @Override
  public void addTo(long[] indices, float[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], get(indices[i]) + values[i]);
    }
  }

  @Override
  public void mergeTo(LongFloatVector mergedRow) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case DENSE: {
        float[] values;
        if (VectorStorageUtils.useIntKey(vector)) {
          values = getIntFloatVector().getStorage().getValues();
        } else {
          values = getLongFloatVector().getStorage().getValues();
        }

        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + indexOffset, values[i]);
        }
      }
      break;

      case SPARSE: {
        if (VectorStorageUtils.useIntKey(vector)) {
          ObjectIterator<Int2FloatMap.Entry> iter = getIntFloatVector().getStorage()
              .entryIterator();
          Int2FloatMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getIntKey() + indexOffset, entry.getFloatValue());
          }
        } else {
          ObjectIterator<Long2FloatMap.Entry> iter =
              getLongFloatVector().getStorage().entryIterator();
          Long2FloatMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getLongKey() + indexOffset, entry.getFloatValue());
          }
        }

        break;
      }

      case SORTED: {
        if (VectorStorageUtils.useIntKey(vector)) {
          int[] indices = getIntFloatVector().getStorage().getIndices();
          float[] values = getIntFloatVector().getStorage().getValues();
          for (int i = 0; i < indices.length; i++) {
            mergedRow.set(indices[i] + indexOffset, values[i]);
          }
        } else {
          long[] indices = getLongFloatVector().getStorage().getIndices();
          float[] values = getLongFloatVector().getStorage().getValues();
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
  public float initAndGet(long index, InitFunc func) {
    if (exist(index)) {
      return get(index);
    } else {
      float value = (float) func.action();
      set(index, value);
      return value;
    }
  }

  @Override
  public void elemUpdate(FloatElemUpdateFunc func) {
    StorageMethod method = VectorStorageUtils.getStorageMethod(vector);
    switch (method) {
      case DENSE:
      case SORTED: {
        // Attention: Only update the exist values for sorted storage method
        float[] values;
        if (VectorStorageUtils.useIntKey(vector)) {
          values = getIntFloatVector().getStorage().getValues();
        } else {
          values = getLongFloatVector().getStorage().getValues();
        }
        for (int i = 0; i < values.length; i++) {
          values[i] = func.update();
        }
      }
      break;

      case SPARSE: {
        // Attention: Only update exist element
        if (VectorStorageUtils.useIntKey(vector)) {
          ObjectIterator<Int2FloatMap.Entry> iter = getIntFloatVector().getStorage()
              .entryIterator();
          Int2FloatMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            entry.setValue(func.update());
          }
        } else {
          ObjectIterator<Long2FloatMap.Entry> iter = getLongFloatVector().getStorage()
              .entryIterator();
          Long2FloatMap.Entry entry;
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
        return getIntFloatVector().getStorage().get((int) (index - indexOffset)) == 0;
      } else {
        return getLongFloatVector().getStorage().get(index - indexOffset) == 0;
      }
    } else {
      // SPARSE and SORT, check index exist or not, When using SORT mode storage, the search efficiency is very low.
      if (VectorStorageUtils.useIntKey(vector)) {
        return getIntFloatVector().getStorage().hasKey((int) (index - indexOffset));
      } else {
        return getLongFloatVector().getStorage().hasKey(index - indexOffset);
      }
    }
  }

  @Override
  public void clear() {
    VectorStorageUtils.clear(vector);
  }

  @Override
  public LongFloatVectorStorage deepClone() {
    return new LongFloatVectorStorage((FloatVector) vector.copy(), indexOffset);
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
  public LongFloatVectorStorage adaptiveClone() {
    if (isSparse()) {
      if (VectorStorageUtils.useIntKey(vector)) {
        return new LongFloatVectorStorage(VFactory.sortedFloatVector((int) vector.dim(),
            ((IntFloatVector) vector).getStorage().getIndices(),
            ((IntFloatVector) vector).getStorage().getValues()), indexOffset);
      } else {
        return new LongFloatVectorStorage(VFactory.sortedLongKeyFloatVector(vector.dim(),
            ((LongFloatVector) vector).getStorage().getIndices(),
            ((LongFloatVector) vector).getStorage().getValues()), indexOffset);
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
    vector = (FloatVector) VectorStorageUtils.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + VectorStorageUtils.bufferLen(vector);
  }
}
