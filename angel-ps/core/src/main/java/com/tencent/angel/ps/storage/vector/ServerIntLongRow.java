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

import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.LongElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The row with "int" index type and "long" value type in PS
 */
public class ServerIntLongRow extends ServerLongRow {
  private static final Log LOG = LogFactory.getLog(ServerIntLongRow.class);
  /**
   * Just a view of "row" in ServerRow
   */
  private IntLongVector intLongRow;

  /**
   * Just a view of "startCol" in ServerRow
   */
  private transient int startColInt;

  /**
   * Just a view of "endCol" in ServerRow
   */
  private transient int endColInt;

  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   * @param innerRow   inner row
   */
  public ServerIntLongRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum,
    IntLongVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
    this.startColInt = startCol;
    this.endColInt = endCol;
    this.intLongRow = (IntLongVector) row;
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
  public ServerIntLongRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new ServerIntLongRow
   *
   * @param rowType row type
   */
  public ServerIntLongRow(RowType rowType) {
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
  public long get(int index) {
    return intLongRow.get(index - startColInt);
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(int index, long value) {
    intLongRow.set(index - startColInt, value);
  }

  /**
   * Get a batch elements values without lock
   *
   * @param indices elements indices
   * @return elements values
   */
  public long[] get(int[] indices) {
    long[] values = new long[indices.length];
    for (int i = 0; i < indices.length; i++) {
      values[i] = intLongRow.get(indices[i] - startColInt);
    }
    return values;
  }

  /**
   * Set a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements values
   */
  public void set(int[] indices, long[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      intLongRow.set(indices[i] - startColInt, values[i]);
    }
  }

  /**
   * Add a element value without lock
   *
   * @param index element index
   * @param value element plus value
   */
  public void addTo(int index, long value) {
    set(index, get(index) + value);
  }

  /**
   * Add a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements plus values
   */
  public void addTo(int[] indices, long[] values) {
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
    return intLongRow.getStorage().getValues();
  }

  /**
   * Get all element indices and values without lock, you must check the storage is sparse first use "isSparse";
   * if you want use original indices, you must plus with "startCol" first
   *
   * @return all element values
   */
  public ObjectIterator<Int2LongMap.Entry> getIter() {
    return intLongRow.getStorage().entryIterator();
  }


  @Override public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();
    try {
      switch (updateType) {
        case T_LONG_SPARSE:
        case T_LONG_SPARSE_COMPONENT:
          updateUseSparse(buf, op);
          break;

        case T_LONG_DENSE:
        case T_LONG_DENSE_COMPONENT:
          updateUseDense(buf, op);
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

  private void updateUseDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        intLongRow.set(i, intLongRow.get(i) + buf.readLong());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intLongRow.set(i, buf.readLong());
      }
    }
  }

  private void updateUseSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        int index = buf.readInt();
        intLongRow.set(index, intLongRow.get(index) + buf.readLong());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intLongRow.set(buf.readInt(), buf.readLong());
      }
    }
  }

  @Override public int size() {
    return intLongRow.size();
  }

  public void mergeTo(IntLongVector mergedRow) {
    startRead();
    try {
      if (isDense()) {
        long[] values = getValues();
        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + startColInt, values[i]);
        }
      } else {
        ObjectIterator<Int2LongMap.Entry> iter = getIter();
        Int2LongMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          mergedRow.set(entry.getIntKey() + startColInt, entry.getLongValue());
        }
      }
    } finally {
      endRead();
    }
  }

  @Override protected void serializeRow(ByteBuf buf) {
    if (isDense()) {
      long[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        buf.writeLong(values[i]);
      }
    } else {
      ObjectIterator<Int2LongMap.Entry> iter = getIter();
      Int2LongMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeLong(entry.getLongValue());
      }
    }
  }

  @Override protected void deserializeRow(ByteBuf buf) {
    startColInt = (int) startCol;
    endColInt = (int) endCol;
    intLongRow = (IntLongVector) row;

    if (isDense()) {
      long[] values = getValues();
      for (int i = 0; i < size; i++) {
        values[i] = buf.readLong();
      }
    } else {
      for (int i = 0; i < size; i++) {
        intLongRow.set(buf.readInt(), buf.readLong());
      }
    }
  }

  @Override public int getRowSpace() {
    if (isDense()) {
      return 8 * size();
    } else {
      return 12 * size();
    }
  }

  @Override public ServerRow clone() {
    startRead();
    try {
      return new ServerIntLongRow(rowId, rowType, startColInt, endColInt, (int) estElemNum,
        intLongRow.clone());
    } finally {
      endRead();
    }
  }

  @Override public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out)
    throws IOException {
    if (indexType == IndexType.INT) {
      for (int i = 0; i < indexSize; i++) {
        out.writeLong(get(in.readInt()));
      }
    } else {
      throw new IOException(this.getClass().getName() + " only support int type index now");
    }
  }

  @Override public void setSplit(Vector row) {
    super.setSplit(row);
    intLongRow = (IntLongVector) row;
  }

  @Override public void elemUpdate(LongElemUpdateFunc func) {
    if (isDense()) {
      long[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        values[i] = func.update();
      }
    } else {
      ObjectIterator<Int2LongMap.Entry> iter = getIter();
      Int2LongMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        entry.setValue(func.update());
      }
    }
  }
}
