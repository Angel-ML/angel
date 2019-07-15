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
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.IOException;

/**
 * The row with "long" index type and "int" value type in PS
 */
public class ServerCompDenseLongIntRow extends ServerRow {

  /**
   * Just a view of "row" in ServerRow
   */
  private IntIntVector intIntRow;


  /**
   * Create a new ServerCompDenseLongIntRow
   *
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   */
  public ServerCompDenseLongIntRow(int rowId, RowType rowType, long startCol, long endCol,
      int estElemNum, IntIntVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
    this.intIntRow = (IntIntVector) row;
  }

  /**
   * Create a new ServerCompDenseLongIntRow
   *
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   */
  public ServerCompDenseLongIntRow(int rowId, RowType rowType, long startCol, long endCol,
      int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new ServerCompDenseLongIntRow
   *
   * @param rowType row type
   */
  public ServerCompDenseLongIntRow(RowType rowType) {
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
    return intIntRow.get((int) (index - startCol));
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(long index, int value) {
    intIntRow.set((int) (index - startCol), value);
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
      values[i] = intIntRow.get((int) (indices[i] - startCol));
    }
    return values;
  }

  /**
   * Set a batch elements values without lock
   *
   * @param indices elements indices
   * @param values elements values
   */
  public void set(long[] indices, int[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      intIntRow.set((int) (indices[i] - startCol), values[i]);
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
   * @param values elements plus values
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
    return intIntRow.getStorage().getValues();
  }

  /**
   * Get all element indices and values without lock, you must check the storage is sparse first use
   * "isSparse"; if you want use original indices, you must plus with "startCol" first
   *
   * @return all element values
   */
  public ObjectIterator<Int2IntMap.Entry> getIter() {
    return intIntRow.getStorage().entryIterator();
  }

  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();

    try {
      switch (updateType) {
        case T_INT_SPARSE_LONGKEY:
        case T_INT_SPARSE_LONGKEY_COMPONENT:
          updateUseSparse(buf, op);
          break;

        case T_INT_DENSE:
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
        int index = (int) buf.readLong();
        intIntRow.set(index, intIntRow.get(index) + buf.readInt());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intIntRow.set((int) buf.readLong(), buf.readInt());
      }
    }
  }


  @Override
  public int size() {
    return intIntRow.size();
  }

  @Override
  protected void serializeRow(ByteBuf buf) {
    int[] values = getValues();
    for (int i = 0; i < values.length; i++) {
      buf.writeInt(values[i]);
    }
  }

  @Override
  protected void deserializeRow(ByteBuf buf) {
    intIntRow = (IntIntVector) row;
    int[] values = getValues();
    for (int i = 0; i < size; i++) {
      values[i] = buf.readInt();
    }
  }

  @Override
  protected int getRowSpace() {
    return size() * 4;
  }

  @Override
  public ServerRow clone() {
    startRead();
    try {
      return new ServerCompDenseLongIntRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          intIntRow.clone());
    } finally {
      endRead();
    }
  }

  @Override
  public ServerRow
adaptiveClone() {
    startRead();
    try {
      return new ServerCompDenseLongIntRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          intIntRow);
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
  public boolean exist(long index) {
    return intIntRow.getStorage().hasKey((int) (index - startCol));
  }

  public int initAndGet(long index, InitFunc func) {
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

  @Override
  public void setSplit(Vector row) {
    super.setSplit(row);
    intIntRow = (IntIntVector) row;
  }
}
