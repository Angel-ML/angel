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
import com.tencent.angel.ps.storage.vector.func.IntElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.IOException;

/**
 * The row with "long" index type and "int" value type in PS
 */
public class ServerLongIntRow extends ServerIntRow {
  /**
   * Create a new ServerIntIntRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   * @param innerRow   the inner row
   */
  public ServerLongIntRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
    IntVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
  }

  /**
   * Create a new ServerIntIntRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   */
  public ServerLongIntRow(int rowId, RowType rowType, long startCol, long endCol,
    int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new ServerLongIntRow
   *
   * @param rowType
   */
  public ServerLongIntRow(RowType rowType) {
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
  public int get(long index) {
    if(useIntKey) {
      return ((IntIntVector) row).get((int)(index - startCol));
    } else {
      return ((LongIntVector) row).get(index - startCol);
    }
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(long index, int value) {
    if(useIntKey) {
      ((IntIntVector) row).set((int)(index - startCol), value);
    } else {
      ((LongIntVector) row).set(index - startCol, value);
    }
  }

  /**
   * Get a batch elements values without lock
   *
   * @param indices elements indices
   * @return elements values
   */
  public int[] get(long[] indices) {
    int[] values = new int[indices.length];
    if(useIntKey) {
      for (int i = 0; i < indices.length; i++) {
        values[i] = ((IntIntVector) row).get((int)(indices[i] - startCol));
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        values[i] = ((LongIntVector) row).get(indices[i] - startCol);
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
  public void set(long[] indices, int[] values) {
    assert indices.length == values.length;
    if(useIntKey) {
      for (int i = 0; i < indices.length; i++) {
        ((IntIntVector) row).set((int)(indices[i] - startCol), values[i]);
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        ((LongIntVector) row).set(indices[i] - startCol, values[i]);
      }
    }
  }

  /**
   * Add a element value without lock
   *
   * @param index element index
   * @param value element plus value
   */
  public void addTo(long index, int value) {
    set(index, get(index) + value);
  }

  /**
   * Add a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements plus values
   */
  public void addTo(long[] indices, int[] values) {
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
  private int[] getValues() {
    if(useIntKey) {
      return ((IntIntVector) row).getStorage().getValues();
    } else {
      return ((LongIntVector) row).getStorage().getValues();
    }
  }

  @Override public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();
    try {
      switch (updateType) {
        case T_INT_SPARSE_LONGKEY:
        case T_INT_SPARSE_LONGKEY_COMPONENT:
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
          ((IntIntVector) row).set(index, ((IntIntVector) row).get(index) + buf.readInt());
        }
      } else {
        for(int i = 0; i < size; i++) {
          long index = buf.readLong();
          ((LongIntVector) row).set(index, ((LongIntVector) row).get(index) + buf.readInt());
        }
      }
    } else {
      if(useIntKey) {
        for(int i = 0; i < size; i++) {
          ((IntIntVector) row).set((int)buf.readLong(), buf.readInt());
        }
      } else {
        for(int i = 0; i < size; i++) {
          ((LongIntVector) row).set(buf.readLong(), buf.readInt());
        }
      }
    }
  }

  @Override public int size() {
    if(useIntKey) {
      return ((IntIntVector) row).size();
    } else {
      return (int)((LongIntVector) row).size();
    }
  }

  /**
   * Merge this row split to a row
   *
   * @param mergedRow the dest row
   */
  public void mergeTo(LongIntVector mergedRow) {
    startRead();
    try {
      if(isDense()) {
        int[] values = getValues();
        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + startCol, values[i]);
        }
      } else {
        if(useIntKey) {
          ObjectIterator<Int2IntMap.Entry> iter = ((IntIntVector) row).getStorage().entryIterator();
          Int2IntMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getIntKey() + startCol, entry.getIntValue());
          }
        } else {
          ObjectIterator<Long2IntMap.Entry> iter = ((LongIntVector) row).getStorage().entryIterator();
          Long2IntMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getLongKey() + startCol, entry.getIntValue());
          }
        }
      }
    } finally {
      endRead();
    }
  }

  @Override protected void serializeRow(ByteBuf buf) {
    if(useIntKeySerialize()) {
      if(useDenseSerialize()) {
        int[] values = getValues();
        for (int i = 0; i < values.length; i++) {
          buf.writeInt(values[i]);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = ((IntIntVector) row).getStorage().entryIterator();
        Int2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          buf.writeInt(entry.getIntKey());
          buf.writeInt(entry.getIntValue());
        }
      }
    } else {
      ObjectIterator<Long2IntMap.Entry> iter = ((LongIntVector) row).getStorage().entryIterator();
      Long2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeInt(entry.getIntValue());
      }
    }
  }

  @Override protected void deserializeRow(ByteBuf buf) {
    if(useIntKeySerialize()) {
      IntIntVector intIntRow = (IntIntVector) row;
      if(useDenseSerialize()) {
        for (int i = 0; i < size; i++) {
          intIntRow.set(i, buf.readInt());
        }
      } else {
        for (int i = 0; i < size; i++) {
          intIntRow.set(buf.readInt(), buf.readInt());
        }
      }
    } else {
      LongIntVector longIntRow = (LongIntVector) row;
      for (int i = 0; i < size; i++) {
        longIntRow.set(buf.readLong(), buf.readInt());
      }
    }
  }

  @Override protected int getRowSpace() {
    if(useIntKeySerialize()) {
      if(useDenseSerialize()) {
        return size * 4;
      } else {
        return size * 8;
      }
    } else {
      return size * 12;
    }
  }

  @Override public ServerRow clone() {
    startRead();
    try {
      if(useIntKey) {
        return new ServerLongIntRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          ((IntIntVector) row).clone());
      } else {
        return new ServerLongIntRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          ((LongIntVector) row).clone());
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
      if(row.isSparse()) {
        return ((IntIntVector) row).getStorage().hasKey((int)(index - startCol));
      } else {
        return ((IntIntVector) row).getStorage().get((int)(index - startCol)) != 0;
      }
    } else {
      if(row.isSparse()) {
        return ((LongIntVector) row).getStorage().hasKey(index - startCol);
      } else {
        return ((LongIntVector) row).getStorage().get(index - startCol) != 0;
      }
    }
  }

  public int initAndGet(long index, InitFunc func) {
    if(exist(index)) {
      return get(index);
    } else {
      int value = (int)func.action();
      set(index, value);
      return value;
    }
  }

  @Override public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func)
    throws IOException {
    if(func != null) {
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

  @Override public void setSplit(Vector row) {
    super.setSplit(row);
  }

  @Override public void elemUpdate(IntElemUpdateFunc func) {
    if(isDense()) {
      int[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        values[i] = func.update();
      }
    } else {
      if(useIntKey) {
        ObjectIterator<Int2IntMap.Entry> iter = ((IntIntVector) row).getStorage().entryIterator();
        Int2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          entry.setValue(func.update());
        }
      } else {
        ObjectIterator<Long2IntMap.Entry> iter = ((LongIntVector) row).getStorage().entryIterator();
        Long2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          entry.setValue(func.update());
        }
      }
    }
  }

}
