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

import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The row with "long" index type and "int" value type in PS
 */
public class ServerLongIntRow extends ServerRow {
  /**
   * Just a view of "row" in ServerRow
   */
  private LongIntVector longIntRow;

  /**
   * Create a new ServerLongIntRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   * @param innerRow   inner row
   */
  public ServerLongIntRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
    LongIntVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
    this.longIntRow = (LongIntVector) row;
  }

  /**
   * Create a new ServerLongIntRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   */
  public ServerLongIntRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new ServerLongIntRow
   *
   * @param rowType row type
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
    return longIntRow.get(index - startCol);
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(long index, int value) {
    longIntRow.set(index - startCol, value);
  }

  /**
   * Get a batch elements values without lock
   *
   * @param indices elements indices
   * @return elements values
   */
  public int[] get(long[] indices) {
    int[] values = new int[indices.length];
    for (int i = 0; i < indices.length; i++) {
      values[i] = longIntRow.get(indices[i] - startCol);
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
    for (int i = 0; i < indices.length; i++) {
      longIntRow.set(indices[i] - startCol, values[i]);
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
  public int[] getValues() {
    return longIntRow.getStorage().getValues();
  }

  /**
   * Get all element indices and values without lock, you must check the storage is sparse first use "isSparse";
   * if you want use original indices, you must plus with "startCol" first
   *
   * @return all element values
   */
  public ObjectIterator<Long2IntMap.Entry> getIter() {
    return longIntRow.getStorage().entryIterator();
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
      for (int i = 0; i < size; i++) {
        long index = buf.readLong();
        longIntRow.set(index, longIntRow.get(index) + buf.readInt());
      }
    } else {
      for (int i = 0; i < size; i++) {
        longIntRow.set(buf.readLong(), buf.readInt());
      }
    }
  }

  @Override public int size() {
    return (int) longIntRow.size();
  }

  public void mergeTo(LongIntVector mergedRow) {
    startRead();
    try {
      if (isSparse()) {
        ObjectIterator<Long2IntMap.Entry> iter = getIter();
        Long2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          mergedRow.set(entry.getLongKey() + startCol, entry.getIntValue());
        }
      }
    } finally {
      endRead();
    }
  }

  @Override protected void serializeRow(ByteBuf buf) {
    if (isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = getIter();
      Long2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeInt(entry.getIntValue());
      }
    }
  }

  @Override protected void deserializeRow(ByteBuf buf) {
    longIntRow = (LongIntVector) row;
    if (isSparse()) {
      for (int i = 0; i < size; i++) {
        longIntRow.set(buf.readLong(), buf.readInt());
      }
    }
  }

  @Override protected int getRowSpace() {
    return size() * 12;
  }

  @Override public ServerRow clone() {
    startRead();
    try {
      return new ServerLongIntRow(rowId, rowType, startCol, endCol, (int) estElemNum,
        longIntRow.clone());
    } finally {
      endRead();
    }
  }

  @Override protected void writeRow(DataOutputStream output) throws IOException {
    if (isSparse()) {
      output.writeInt(size());
      ObjectIterator<Long2IntMap.Entry> iter = getIter();
      Long2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        output.writeLong(entry.getLongKey());
        output.writeInt(entry.getIntValue());
      }
    }
  }

  @Override protected void readRow(DataInputStream input) throws IOException {
    longIntRow = (LongIntVector) row;
    if (isSparse()) {
      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        longIntRow.set(input.readLong(), input.readInt());
      }
    }
  }

  @Override public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out)
    throws IOException {
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

  @Override public void setSplit(Vector row) {
    super.setSplit(row);
    longIntRow = (LongIntVector) row;
  }
}
