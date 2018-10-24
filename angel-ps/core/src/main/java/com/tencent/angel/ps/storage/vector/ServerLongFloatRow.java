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


package com.tencent.angel.ps.storage.vector;

import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.FloatElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.IOException;

/**
 * The row with "long" index type and "float" value type in PS
 */
public class ServerLongFloatRow extends ServerFloatRow {
  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   */
  public ServerLongFloatRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
    FloatVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
  }

  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   */
  public ServerLongFloatRow(int rowId, RowType rowType, long startCol, long endCol,
    int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new ServerLongFloatRow
   *
   * @param rowType row type
   */
  public ServerLongFloatRow(RowType rowType) {
    this(0, rowType, 0, 0, 0);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  //Methods with out lock operation, you must call startWrite/startRead before using these methods
  // and call endWrite/endRead after
  //////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Get a element value with out lock operation
   *
   * @param index element index
   * @return element value
   */
  public float get(long index) {
    if(useIntKey) {
      return ((IntFloatVector) row).get((int)(index - startCol));
    } else {
      return ((LongFloatVector) row).get(index - startCol);
    }
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(long index, float value) {
    if(useIntKey) {
      ((IntFloatVector) row).set((int)(index - startCol), value);
    } else {
      ((LongFloatVector) row).set(index - startCol, value);
    }
  }

  /**
   * Get a batch elements values without lock
   *
   * @param indices elements indices
   * @return elements values
   */
  public float[] get(long[] indices) {
    float[] values = new float[indices.length];
    if(useIntKey) {
      for (int i = 0; i < indices.length; i++) {
        values[i] = ((IntFloatVector) row).get((int)(indices[i] - startCol));
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        values[i] = ((LongFloatVector) row).get(indices[i] - startCol);
      }
    }

    return values;
  }

  /**
   * Set a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements values
   */
  public void set(long[] indices, float[] values) {
    assert indices.length == values.length;
    if(useIntKey) {
      for (int i = 0; i < indices.length; i++) {
        ((IntFloatVector) row).set((int)(indices[i] - startCol), values[i]);
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        ((LongFloatVector) row).set(indices[i] - startCol, values[i]);
      }
    }
  }

  /**
   * Add a element value without lock
   *
   * @param index element index
   * @param value element plus value
   */
  public void addTo(long index, float value) {
    set(index, get(index) + value);
  }

  /**
   * Add a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements plus values
   */
  public void addTo(long[] indices, float[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      set(indices[i], get(indices[i]) + values[i]);
    }
  }

  /**
   * Get all element values without lock, you must check the storage is dense first use "isDense"
   *
   * @return all element values
   */
  private float[] getValues() {
    if(useIntKey) {
      return ((IntFloatVector) row).getStorage().getValues();
    } else {
      return ((LongFloatVector) row).getStorage().getValues();
    }
  }

  /**
   * Get all element indices and values without lock, you must check the storage is sparse first use "isSparse";
   * if you want use original indices, you must plus with "startCol" first
   *
   * @return all element values
   */
  //public ObjectIterator<Long2FloatMap.Entry> getIter() {
  //  return ((LongFloatVector) row).getStorage().entryIterator();
  //}


  @Override public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();
    try {
      switch (updateType) {
        case T_FLOAT_SPARSE_LONGKEY:
        case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
          updateUseSparse(buf, op);
          break;

        default: {
          throw new UnsupportedOperationException(
            "Unsupport operation: update " + updateType + " to " + this.getClass().getName());
        }
      }

      updateRowVersion();
    } finally {
      endWrite();
    }
  }

  private void updateUseSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      if(useIntKey) {
        for(int i = 0; i < size; i++) {
          int index = (int)buf.readLong();
          ((IntFloatVector) row).set(index, ((IntFloatVector) row).get(index) + buf.readFloat());
        }
      } else {
        for(int i = 0; i < size; i++) {
          long index = buf.readLong();
          ((LongFloatVector) row).set(index, ((LongFloatVector) row).get(index) + buf.readFloat());
        }
      }
    } else {
      if(useIntKey) {
        for(int i = 0; i < size; i++) {
          int index = (int)buf.readLong();
          ((IntFloatVector) row).set(index, buf.readFloat());
        }
      } else {
        for(int i = 0; i < size; i++) {
          long index = buf.readLong();
          ((LongFloatVector) row).set(index, buf.readFloat());
        }
      }
    }
  }


  @Override public int size() {
    if(useIntKey) {
      return ((IntFloatVector) row).size();
    } else {
      return (int)((LongFloatVector) row).size();
    }
  }

  public void mergeTo(LongFloatVector mergedRow) {
    startRead();
    try {
      if(isDense()) {
        float[] values = getValues();
        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + startCol, values[i]);
        }
      } else {
        if(useIntKey) {
          ObjectIterator<Int2FloatMap.Entry> iter = ((IntFloatVector) row).getStorage().entryIterator();
          Int2FloatMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getIntKey() + startCol, entry.getFloatValue());
          }
        } else {
          ObjectIterator<Long2FloatMap.Entry> iter = ((LongFloatVector) row).getStorage().entryIterator();
          Long2FloatMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getLongKey() + startCol, entry.getFloatValue());
          }
        }
      }
    } finally {
      endRead();
    }
  }

  @Override protected void serializeRow(ByteBuf buf) {
    if(isDense()) {
      float[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        buf.writeLong(i);
        buf.writeFloat(values[i]);
      }
    } else {
      if(useIntKey) {
        ObjectIterator<Int2FloatMap.Entry> iter = ((IntFloatVector) row).getStorage().entryIterator();
        Int2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          buf.writeLong(entry.getIntKey());
          buf.writeFloat(entry.getFloatValue());
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = ((LongFloatVector) row).getStorage().entryIterator();
        Long2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          buf.writeLong(entry.getLongKey());
          buf.writeFloat(entry.getFloatValue());
        }
      }
    }
  }

  @Override protected void deserializeRow(ByteBuf buf) {
    if(useIntKey) {
      IntFloatVector IntFloatRow = (IntFloatVector) row;
      for (int i = 0; i < size; i++) {
        IntFloatRow.set((int)buf.readLong(), buf.readFloat());
      }
    } else {
      LongFloatVector longFloatRow = (LongFloatVector) row;
      for (int i = 0; i < size; i++) {
        longFloatRow.set(buf.readLong(), buf.readFloat());
      }
    }
  }

  @Override protected int getRowSpace() {
    return size() * 12;
  }

  @Override public ServerRow clone() {
    startRead();
    try {
      if(useIntKey) {
        return new ServerLongFloatRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          ((IntFloatVector) row).clone());
      } else {
        return new ServerLongFloatRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          ((LongFloatVector) row).clone());
      }
    } finally {
      endRead();
    }
  }

  /**
   * Check the vector contains the index or not
   * @param index element index
   * @return true means exist
   */
  public boolean exist(long index) {
    if(useIntKey) {
      return ((IntFloatVector) row).getStorage().hasKey((int)(index - startCol));
    } else {
      return ((LongFloatVector) row).getStorage().hasKey(index - startCol);
    }
  }

  public float initAndGet(long index, InitFunc func) {
    if(exist(index)) {
      return get(index);
    } else {
      float value = (float)func.action();
      set(index, value);
      return value;
    }
  }

  @Override public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func)
    throws IOException {
    if(func != null) {
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

  @Override public void setSplit(Vector row) {
    super.setSplit(row);
  }

  @Override public void elemUpdate(FloatElemUpdateFunc func) {
    if(isDense()) {
      float[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        values[i] = func.update();
      }
    } else {
      if(useIntKey) {
        ObjectIterator<Int2FloatMap.Entry> iter = ((IntFloatVector) row).getStorage().entryIterator();
        Int2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          entry.setValue(func.update());
        }
      } else {
        ObjectIterator<Long2FloatMap.Entry> iter = ((LongFloatVector) row).getStorage().entryIterator();
        Long2FloatMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          entry.setValue(func.update());
        }
      }
    }
  }
}
