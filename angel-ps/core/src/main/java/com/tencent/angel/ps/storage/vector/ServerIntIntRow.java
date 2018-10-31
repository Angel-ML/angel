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
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.IntElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The row with "int" index type and "int" value type in PS
 */
public class ServerIntIntRow extends ServerIntRow {
  /**
   * Just a view of "row" in ServerRow
   */
  private IntIntVector intIntRow;
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
  public ServerIntIntRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum,
    IntIntVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
    this.startColInt = startCol;
    this.endColInt = endCol;
    this.intIntRow = (IntIntVector) row;
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
  public ServerIntIntRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a empty ServerIntIntRow
   *
   * @param rowType row type
   */
  public ServerIntIntRow(RowType rowType) {
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
  public int get(int index) {
    return intIntRow.get(index - startColInt);
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(int index, int value) {
    intIntRow.set(index - startColInt, value);
  }

  /**
   * Get a batch elements values without lock
   *
   * @param indices elements indices
   * @return elements values
   */
  public int[] get(int[] indices) {
    int[] values = new int[indices.length];
    for (int i = 0; i < indices.length; i++) {
      values[i] = intIntRow.get(indices[i] - startColInt);
    }
    return values;
  }

  /**
   * Set a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements values
   */
  public void set(int[] indices, int[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      intIntRow.set(indices[i] - startColInt, values[i]);
    }
  }

  /**
   * Add a element value without lock
   *
   * @param index element index
   * @param value element plus value
   */
  public void addTo(int index, int value) {
    set(index, get(index) + value);
  }

  /**
   * Add a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements plus values
   */
  public void addTo(int[] indices, int[] values) {
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
    return intIntRow.getStorage().getValues();
  }

  /**
   * Get all element indices and values without lock, you must check the storage is sparse first use "isSparse";
   * if you want use original indices, you must plus with "startCol" first
   *
   * @return all element values
   */
  public ObjectIterator<Int2IntMap.Entry> getIter() {
    return intIntRow.getStorage().entryIterator();
  }

  @Override public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();
    try {
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

      updateRowVersion();
    } finally {
      endWrite();
    }
  }

  private void updateUseDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        intIntRow.set(i, intIntRow.get(i) + buf.readInt());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intIntRow.set(i, buf.readInt());
      }
    }
  }

  private void updateUseSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        int index = buf.readInt();
        intIntRow.set(index, intIntRow.get(index) + buf.readInt());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intIntRow.set(buf.readInt(), buf.readInt());
      }
    }
  }

  @Override public int size() {
    return intIntRow.size();
  }

  public void mergeTo(IntIntVector mergedRow) {
    startRead();
    try {
      if (isDense()) {
        int[] values = getValues();
        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + startColInt, values[i]);
        }
      } else {
        ObjectIterator<Int2IntMap.Entry> iter = getIter();
        Int2IntMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          mergedRow.set(entry.getIntKey() + startColInt, entry.getIntValue());
        }
      }
    } finally {
      endRead();
    }
  }

  @Override protected void serializeRow(ByteBuf buf) {
    if (useDenseSerialize()) {
      int[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        buf.writeInt(values[i]);
      }
    } else {
      ObjectIterator<Int2IntMap.Entry> iter = getIter();
      Int2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeInt(entry.getIntValue());
      }
    }
  }

  @Override protected void deserializeRow(ByteBuf buf) {
    startColInt = (int) startCol;
    endColInt = (int) endCol;
    intIntRow = (IntIntVector) row;
    if (useDenseSerialize()) {
      for (int i = 0; i < size; i++) {
        intIntRow.set(i, buf.readInt());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intIntRow.set(buf.readInt(), buf.readInt());
      }
    }
  }

  @Override public int getRowSpace() {
    if (useDenseSerialize()) {
      return 4 * size();
    } else {
      return 8 * size();
    }
  }

  @Override public ServerRow clone() {
    startRead();
    try {
      return new ServerIntIntRow(rowId, rowType, startColInt, endColInt, (int) estElemNum,
        intIntRow.clone());
    } finally {
      endRead();
    }
  }

  /**
   * Check the vector contains the index or not
   *
   * @param index element index
   * @return true means exist
   */
  public boolean exist(int index) {
    if (intIntRow.isSparse()) {
      return intIntRow.getStorage().hasKey(index - startColInt);
    } else {
      return intIntRow.get(index - startColInt) != 0;
    }
  }

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
  public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func)
    throws IOException {
    if (func != null) {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeInt(initAndGet(in.readInt(), func));
        }
      } else {
        throw new IOException(this.getClass().getName() + " only support int type index now");
      }
    } else {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeInt(get(in.readInt()));
        }
      } else {
        throw new IOException(this.getClass().getName() + " only support int type index now");
      }
    }
  }

  @Override public void setSplit(Vector row) {
    super.setSplit(row);
    intIntRow = (IntIntVector) row;
  }

  @Override public void elemUpdate(IntElemUpdateFunc func) {
    if (isDense()) {
      int[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        values[i] = func.update();
      }
    } else {
      ObjectIterator<Int2IntMap.Entry> iter = getIter();
      Int2IntMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        entry.setValue(func.update());
      }
    }
  }
}
