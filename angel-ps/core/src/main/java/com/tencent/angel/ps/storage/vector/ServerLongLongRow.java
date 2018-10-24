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
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.IOException;

/**
 * The row with "long" index type and "long" value type in PS
 */
public class ServerLongLongRow extends ServerRow {
  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   */
  public ServerLongLongRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
    LongVector innerRow) {
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
  public ServerLongLongRow(int rowId, RowType rowType, long startCol, long endCol,
    int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new ServerLongLongRow
   *
   * @param rowType row type
   */
  public ServerLongLongRow(RowType rowType) {
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
  public long get(long index) {
    if(useIntKey) {
      return ((IntLongVector) row).get((int)(index - startCol));
    } else {
      return ((LongLongVector) row).get(index - startCol);
    }
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(long index, long value) {
    if(useIntKey) {
      ((IntLongVector) row).set((int)(index - startCol), value);
    } else {
      ((LongLongVector) row).set(index - startCol, value);
    }
  }

  /**
   * Get a batch elements values without lock
   *
   * @param indices elements indices
   * @return elements values
   */
  public long[] get(long[] indices) {
    long[] values = new long[indices.length];
    if(useIntKey) {
      for (int i = 0; i < indices.length; i++) {
        values[i] = ((IntLongVector) row).get((int)(indices[i] - startCol));
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        values[i] = ((LongLongVector) row).get(indices[i] - startCol);
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
  public void set(long[] indices, long[] values) {
    assert indices.length == values.length;
    if(useIntKey) {
      for (int i = 0; i < indices.length; i++) {
        ((IntLongVector) row).set((int)(indices[i] - startCol), values[i]);
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        ((LongLongVector) row).set(indices[i] - startCol, values[i]);
      }
    }
  }

  /**
   * Add a element value without lock
   *
   * @param index element index
   * @param value element plus value
   */
  public void addTo(long index, long value) {
    set(index, get(index) + value);
  }

  /**
   * Add a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements plus values
   */
  public void addTo(long[] indices, long[] values) {
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
  private long[] getValues() {
    if(useIntKey) {
      return ((IntLongVector) row).getStorage().getValues();
    } else {
      return ((LongLongVector) row).getStorage().getValues();
    }
  }

  /**
   * Get all element indices and values without lock, you must check the storage is sparse first use "isSparse";
   * if you want use original indices, you must plus with "startCol" first
   *
   * @return all element values
   */
  //public ObjectIterator<Long2LongMap.Entry> getIter() {
  //  return ((LongLongVector) row).getStorage().entryIterator();
  //}


  @Override public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();
    try {
      switch (updateType) {
        case T_LONG_SPARSE_LONGKEY:
        case T_LONG_SPARSE_LONGKEY_COMPONENT:
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
          ((IntLongVector) row).set(index, ((IntLongVector) row).get(index) + buf.readLong());
        }
      } else {
        for(int i = 0; i < size; i++) {
          long index = buf.readLong();
          ((LongLongVector) row).set(index, ((LongLongVector) row).get(index) + buf.readLong());
        }
      }
    } else {
      if(useIntKey) {
        for(int i = 0; i < size; i++) {
          int index = (int)buf.readLong();
          ((IntLongVector) row).set(index, buf.readLong());
        }
      } else {
        for(int i = 0; i < size; i++) {
          long index = buf.readLong();
          ((LongLongVector) row).set(index, buf.readLong());
        }
      }
    }
  }


  @Override public int size() {
    if(useIntKey) {
      return ((IntLongVector) row).size();
    } else {
      return (int)((LongLongVector) row).size();
    }
  }

  public void mergeTo(LongLongVector mergedRow) {
    startRead();
    try {
      if(isDense()) {
        long[] values = getValues();
        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + startCol, values[i]);
        }
      } else {
        if(useIntKey) {
          ObjectIterator<Int2LongMap.Entry> iter = ((IntLongVector) row).getStorage().entryIterator();
          Int2LongMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getIntKey() + startCol, entry.getLongValue());
          }
        } else {
          ObjectIterator<Long2LongMap.Entry> iter = ((LongLongVector) row).getStorage().entryIterator();
          Long2LongMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getLongKey() + startCol, entry.getLongValue());
          }
        }
      }
    } finally {
      endRead();
    }
  }

  @Override protected void serializeRow(ByteBuf buf) {
    if(isDense()) {
      long[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        buf.writeLong(i);
        buf.writeLong(values[i]);
      }
    } else {
      if(useIntKey) {
        ObjectIterator<Int2LongMap.Entry> iter = ((IntLongVector) row).getStorage().entryIterator();
        Int2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          buf.writeLong(entry.getIntKey());
          buf.writeLong(entry.getLongValue());
        }
      } else {
        ObjectIterator<Long2LongMap.Entry> iter = ((LongLongVector) row).getStorage().entryIterator();
        Long2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          buf.writeLong(entry.getLongKey());
          buf.writeLong(entry.getLongValue());
        }
      }
    }
  }

  @Override protected void deserializeRow(ByteBuf buf) {
    if(useIntKey) {
      IntLongVector IntLongRow = (IntLongVector) row;
      for (int i = 0; i < size; i++) {
        IntLongRow.set((int)buf.readLong(), buf.readLong());
      }
    } else {
      LongLongVector longLongRow = (LongLongVector) row;
      for (int i = 0; i < size; i++) {
        longLongRow.set(buf.readLong(), buf.readLong());
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
        return new ServerLongLongRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          ((IntLongVector) row).clone());
      } else {
        return new ServerLongLongRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          ((LongLongVector) row).clone());
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
        return ((IntLongVector) row).getStorage().hasKey((int)(index - startCol));
      } else {
        return ((IntLongVector) row).getStorage().get((int)(index - startCol)) != 0;
      }
    } else {
      if(row.isSparse()) {
        return ((LongLongVector) row).getStorage().hasKey(index - startCol);
      } else {
        return ((LongLongVector) row).getStorage().get(index - startCol) != 0;
      }
    }
  }

  public long initAndGet(long index, InitFunc func) {
    if(exist(index)) {
      return get(index);
    } else {
      long value = (long)func.action();
      set(index, value);
      return value;
    }
  }

  @Override public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func)
    throws IOException {
    if(func != null) {
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

  @Override public void setSplit(Vector row) {
    super.setSplit(row);
  }
}
