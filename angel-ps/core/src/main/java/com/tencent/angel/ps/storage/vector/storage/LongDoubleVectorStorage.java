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
import com.tencent.angel.ml.math2.vector.DoubleVector;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.DoubleElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * A long key double value storage: use a IntFloatVector as storage
 */
public class LongDoubleVectorStorage extends LongDoubleStorage {

  /**
   * A vector storage: it can be IntDoubleVector or LongDoubleVector and can use DENSE,SPARSE and
   * SORTED storage type
   */
  private DoubleVector vector;

  public LongDoubleVectorStorage(DoubleVector vector, long indexOffset) {
    super(indexOffset);
    this.vector = vector;
  }

  public LongDoubleVectorStorage() {
    this(null, 0L);
  }


  public DoubleVector getVector() {
    return vector;
  }

  public void setVector(DoubleVector vector) {
    this.vector = vector;
  }

  private LongDoubleVector getLongDoubleVector() {
    return (LongDoubleVector) vector;
  }

  private IntDoubleVector getIntDoubleVector() {
    return (IntDoubleVector) vector;
  }


  @Override
  public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    if (func != null) {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeDouble(initAndGet(in.readInt(), func));
        }
      } else {
        for (int i = 0; i < indexSize; i++) {
          out.writeDouble(initAndGet(in.readLong(), func));
        }
      }
    } else {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeDouble(get(in.readInt()));
        }
      } else {
        for (int i = 0; i < indexSize; i++) {
          out.writeDouble(get(in.readLong()));
        }
      }
    }
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    switch (updateType) {
      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        updateUseLongDoubleSparse(buf, op);
        break;

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

      case T_DOUBLE_SPARSE:
      case T_DOUBLE_SPARSE_COMPONENT:
        updateUseIntDoubleSparse(buf, op);
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

      case T_DOUBLE_DENSE:
      case T_DOUBLE_DENSE_COMPONENT:
        updateUseIntDoubleDense(buf, op);
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

  private void updateUseLongDoubleSparse(ByteBuf buf, UpdateOp op) {

    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntDoubleVector().set(index, getIntDoubleVector().get(index) + buf.readDouble());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongDoubleVector()
              .set(index, getLongDoubleVector().get(index) + buf.readDouble());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set((int) buf.readLong(), buf.readDouble());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(buf.readLong(), buf.readDouble());
        }
      }
    }
  }

  private void updateUseLongFloatSparse(ByteBuf buf, UpdateOp op) {

    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = (int) buf.readLong();
          getIntDoubleVector().set(index, getIntDoubleVector().get(index) + buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongDoubleVector()
              .set(index, getLongDoubleVector().get(index) + buf.readFloat());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set((int) buf.readLong(), buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(buf.readLong(), buf.readFloat());
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
          getIntDoubleVector().set(index, getIntDoubleVector().get(index) + buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongDoubleVector()
              .set(index, getLongDoubleVector().get(index) + buf.readLong());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set((int) buf.readLong(), buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(buf.readLong(), buf.readLong());
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
          getIntDoubleVector().set(index, getIntDoubleVector().get(index) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readLong();
          getLongDoubleVector()
              .set(index, getLongDoubleVector().get(index) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set((int) buf.readLong(), buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(buf.readLong(), buf.readInt());
        }
      }
    }
  }


  private void updateUseIntDoubleSparse(ByteBuf buf, UpdateOp op) {

    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          int index = buf.readInt();
          getIntDoubleVector().set(index, getIntDoubleVector().get(index) + buf.readDouble());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongDoubleVector()
              .set(index, getLongDoubleVector().get(index) + buf.readDouble());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(buf.readInt(), buf.readDouble());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(buf.readInt(), buf.readDouble());
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
          getIntDoubleVector().set(index, getIntDoubleVector().get(index) + buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongDoubleVector()
              .set(index, getLongDoubleVector().get(index) + buf.readFloat());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(buf.readInt(), buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(buf.readInt(), buf.readFloat());
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
          getIntDoubleVector().set(index, getIntDoubleVector().get(index) + buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongDoubleVector()
              .set(index, getLongDoubleVector().get(index) + buf.readLong());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(buf.readInt(), buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(buf.readInt(), buf.readLong());
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
          getIntDoubleVector().set(index, getIntDoubleVector().get(index) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          long index = buf.readInt();
          getLongDoubleVector()
              .set(index, getLongDoubleVector().get(index) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(buf.readInt(), buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(buf.readInt(), buf.readInt());
        }
      }
    }
  }

  private void updateUseIntDoubleDense(ByteBuf buf, UpdateOp op) {

    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(i, getIntDoubleVector().get(i) + buf.readDouble());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector()
              .set(i, getLongDoubleVector().get(i) + buf.readDouble());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(i, buf.readDouble());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(i, buf.readDouble());
        }
      }
    }
  }

  private void updateUseIntFloatDense(ByteBuf buf, UpdateOp op) {

    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(i, getIntDoubleVector().get(i) + buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector()
              .set(i, getLongDoubleVector().get(i) + buf.readFloat());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(i, buf.readFloat());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(i, buf.readFloat());
        }
      }
    }
  }

  private void updateUseIntLongDense(ByteBuf buf, UpdateOp op) {

    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(i, getIntDoubleVector().get(i) + buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector()
              .set(i, getLongDoubleVector().get(i) + buf.readLong());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(i, buf.readLong());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(i, buf.readLong());
        }
      }
    }
  }

  private void updateUseIntIntDense(ByteBuf buf, UpdateOp op) {

    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(i, getIntDoubleVector().get(i) + buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector()
              .set(i, getLongDoubleVector().get(i) + buf.readInt());
        }
      }
    } else {
      if (VectorStorageUtils.useIntKey(vector)) {
        for (int i = 0; i < size; i++) {
          getIntDoubleVector().set(i, buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          getLongDoubleVector().set(i, buf.readInt());
        }
      }
    }
  }


  @Override
  public double get(long index) {
    if (VectorStorageUtils.useIntKey(vector)) {
      return getIntDoubleVector().get((int) (index - indexOffset));
    } else {
      return getLongDoubleVector().get(index - indexOffset);
    }
  }

  @Override
  public void set(long index, double value) {
    if (VectorStorageUtils.useIntKey(vector)) {
      getIntDoubleVector().set((int) (index - indexOffset), value);
    } else {
      getLongDoubleVector().set(index - indexOffset, value);
    }
  }

  @Override
  public double[] get(long[] indices) {
    double[] values = new double[indices.length];
    if (VectorStorageUtils.useIntKey(vector)) {
      for (int i = 0; i < indices.length; i++) {
        values[i] = getIntDoubleVector().get((int) (indices[i] - indexOffset));
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        values[i] = getLongDoubleVector().get(indices[i] - indexOffset);
      }
    }

    return values;
  }

  @Override
  public void set(long[] indices, double[] values) {
    assert indices.length == values.length;
    if (VectorStorageUtils.useIntKey(vector)) {
      for (int i = 0; i < indices.length; i++) {
        getIntDoubleVector().set((int) (indices[i] - indexOffset), values[i]);
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        getLongDoubleVector().set(indices[i] - indexOffset, values[i]);
      }
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
      case DENSE: {
        double[] values;
        if (VectorStorageUtils.useIntKey(vector)) {
          values = getIntDoubleVector().getStorage().getValues();
        } else {
          values = getLongDoubleVector().getStorage().getValues();
        }

        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + indexOffset, values[i]);
        }
      }
      break;

      case SPARSE: {
        if (VectorStorageUtils.useIntKey(vector)) {
          ObjectIterator<Int2DoubleMap.Entry> iter = getIntDoubleVector().getStorage()
              .entryIterator();
          Int2DoubleMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getIntKey() + indexOffset, entry.getDoubleValue());
          }
        } else {
          ObjectIterator<Long2DoubleMap.Entry> iter =
              getLongDoubleVector().getStorage().entryIterator();
          Long2DoubleMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getLongKey() + indexOffset, entry.getDoubleValue());
          }
        }

        break;
      }

      case SORTED: {
        if (VectorStorageUtils.useIntKey(vector)) {
          int[] indices = getIntDoubleVector().getStorage().getIndices();
          double[] values = getIntDoubleVector().getStorage().getValues();
          for (int i = 0; i < indices.length; i++) {
            mergedRow.set(indices[i] + indexOffset, values[i]);
          }
        } else {
          long[] indices = getLongDoubleVector().getStorage().getIndices();
          double[] values = getLongDoubleVector().getStorage().getValues();
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
      case DENSE:
      case SORTED: {
        // Attention: Only update the exist values for sorted storage method
        double[] values;
        if (VectorStorageUtils.useIntKey(vector)) {
          values = getIntDoubleVector().getStorage().getValues();
        } else {
          values = getLongDoubleVector().getStorage().getValues();
        }
        for (int i = 0; i < values.length; i++) {
          values[i] = func.update();
        }
      }
      break;

      case SPARSE: {
        // Attention: Only update exist element
        if (VectorStorageUtils.useIntKey(vector)) {
          ObjectIterator<Int2DoubleMap.Entry> iter = getIntDoubleVector().getStorage()
              .entryIterator();
          Int2DoubleMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            entry.setValue(func.update());
          }
        } else {
          ObjectIterator<Long2DoubleMap.Entry> iter = getLongDoubleVector().getStorage()
              .entryIterator();
          Long2DoubleMap.Entry entry;
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
        return getIntDoubleVector().getStorage().get((int) (index - indexOffset)) == 0;
      } else {
        return getLongDoubleVector().getStorage().get(index - indexOffset) == 0;
      }
    } else {
      // SPARSE and SORT, check index exist or not, When using SORT mode storage, the search efficiency is very low.
      if (VectorStorageUtils.useIntKey(vector)) {
        return getIntDoubleVector().getStorage().hasKey((int) (index - indexOffset));
      } else {
        return getLongDoubleVector().getStorage().hasKey(index - indexOffset);
      }
    }
  }

  @Override
  public void clear() {
    VectorStorageUtils.clear(vector);
  }

  @Override
  public LongDoubleVectorStorage deepClone() {
    return new LongDoubleVectorStorage((DoubleVector) vector.copy(), indexOffset);
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
      if (VectorStorageUtils.useIntKey(vector)) {
        return new LongDoubleVectorStorage(VFactory.sortedDoubleVector((int) vector.dim(),
            ((IntDoubleVector) vector).getStorage().getIndices(),
            ((IntDoubleVector) vector).getStorage().getValues()), indexOffset);
      } else {
        return new LongDoubleVectorStorage(VFactory.sortedLongKeyDoubleVector(vector.dim(),
            ((LongDoubleVector) vector).getStorage().getIndices(),
            ((LongDoubleVector) vector).getStorage().getValues()), indexOffset);
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
    vector = (DoubleVector) VectorStorageUtils.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + VectorStorageUtils.bufferLen(vector);
  }
}
