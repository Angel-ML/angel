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

import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The row with "long" index type and "float" value type in PS
 */
public class ServerCompDenseLongFloatRow extends ServerRow {
  /**
   * Just a view of "row" in ServerRow
   */
  private IntFloatVector intFloatRow;

  /**
   * Create a new ServerCompDenseLongFloatRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   */
  public ServerCompDenseLongFloatRow(int rowId, RowType rowType, long startCol, long endCol,
    int estElemNum, IntFloatVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
    this.intFloatRow = (IntFloatVector) row;
  }

  /**
   * Create a new ServerCompDenseLongFloatRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   */
  public ServerCompDenseLongFloatRow(int rowId, RowType rowType, long startCol, long endCol,
    int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }


  /**
   * Create a new ServerCompDenseLongFloatRow
   *
   * @param rowType row type
   */
  public ServerCompDenseLongFloatRow(RowType rowType) {
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
    return intFloatRow.get((int) (index - startCol));
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(long index, float value) {
    intFloatRow.set((int) (index - startCol), value);
  }

  /**
   * Get a batch elements values without lock
   *
   * @param indices elements indices
   * @return elements values
   */
  public float[] get(long[] indices) {
    float[] values = new float[indices.length];
    for (int i = 0; i < indices.length; i++) {
      values[i] = intFloatRow.get((int) (indices[i] - startCol));
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
    for (int i = 0; i < indices.length; i++) {
      intFloatRow.set((int) (indices[i] - startCol), values[i]);
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
    return intFloatRow.getStorage().getValues();
  }

  /**
   * Get all element indices and values without lock, you must check the storage is sparse first use "isSparse";
   * if you want use original indices, you must plus with "startCol" first
   *
   * @return all element values
   */
  public ObjectIterator<Int2FloatMap.Entry> getIter() {
    return intFloatRow.getStorage().entryIterator();
  }

  @Override public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();

    try {
      switch (updateType) {
        case T_FLOAT_SPARSE_LONGKEY:
        case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
          updateUseSparse(buf, op);
          break;

        case T_FLOAT_DENSE:
          plusToUseDense(buf, op);
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

  private void plusToUseDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        intFloatRow.set(i, intFloatRow.get(i) + buf.readFloat());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intFloatRow.set(i, buf.readFloat());
      }
    }
  }

  private void updateUseSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        int index = (int) buf.readLong();
        intFloatRow.set(index, intFloatRow.get(index) + buf.readFloat());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intFloatRow.set((int) buf.readLong(), buf.readFloat());
      }
    }
  }


  @Override public int size() {
    return intFloatRow.size();
  }

  @Override protected void serializeRow(ByteBuf buf) {
    float[] values = getValues();
    for (int i = 0; i < values.length; i++) {
      buf.writeFloat(values[i]);
    }
  }

  @Override protected void deserializeRow(ByteBuf buf) {
    intFloatRow = (IntFloatVector) row;
    for (int i = 0; i < size; i++) {
      intFloatRow.set(i, buf.readFloat());
    }
  }

  @Override protected int getRowSpace() {
    return size() * 4;
  }

  @Override public ServerRow clone() {
    startRead();
    try {
      return new ServerCompDenseLongFloatRow(rowId, rowType, startCol, endCol, (int) estElemNum,
        intFloatRow.clone());
    } finally {
      endRead();
    }
  }

  @Override public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out)
    throws IOException {
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

  @Override public void setSplit(Vector row) {
    super.setSplit(row);
    intFloatRow = (IntFloatVector) row;
  }
}
