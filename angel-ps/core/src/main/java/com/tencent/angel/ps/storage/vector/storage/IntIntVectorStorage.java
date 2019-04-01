package com.tencent.angel.ps.storage.vector.storage;

import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.IntElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;


public class IntIntVectorStorage extends IntIntStorage {

  private IntIntVector vector;

  public IntIntVectorStorage(IntIntVector vector, long indexOffset) {
    super(indexOffset);
    this.vector = vector;
  }

  public IntIntVectorStorage(){
    this(null, 0L);
  }

  public IntIntVector getVector() {
    return vector;
  }

  public void setVector(IntIntVector vector) {
    this.vector = vector;
  }

  @Override
  public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    if(indexType != IndexType.INT) {
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

  private void updateUseSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    int index;
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        index = buf.readInt();
        getVector().set(index, getVector().get(index) + buf.readInt());
      }
    } else {
      for (int i = 0; i < size; i++) {
        getVector().set(buf.readInt(), buf.readInt());
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
  public int get(int index) {
    return getVector().get(index - (int) indexOffset);
  }

  @Override
  public void set(int index, int value) {
    getVector().set(index - (int) indexOffset, value);
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
