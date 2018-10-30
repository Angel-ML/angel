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
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.DoubleElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.IOException;

/**
 * The row with "int" index type and "double" value type in PS
 */
public class ServerIntDoubleRow extends ServerDoubleRow {
  /**
   * Just a view of "row" in ServerRow
   */
  private IntDoubleVector intDoubleRow;

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
  public ServerIntDoubleRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum,
    IntDoubleVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
    this.startColInt = startCol;
    this.endColInt = endCol;
    this.intDoubleRow = (IntDoubleVector) row;
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
  public ServerIntDoubleRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowType row type
   */
  public ServerIntDoubleRow(RowType rowType) {
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
  public double get(int index) {
    return intDoubleRow.get(index - startColInt);
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(int index, double value) {
    intDoubleRow.set(index - startColInt, value);
  }

  /**
   * Get a batch elements values without lock
   *
   * @param indices elements indices
   * @return elements values
   */
  public double[] get(int[] indices) {
    double[] values = new double[indices.length];
    for (int i = 0; i < indices.length; i++) {
      values[i] = intDoubleRow.get(indices[i] - startColInt);
    }
    return values;
  }

  /**
   * Set a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements values
   */
  public void set(int[] indices, double[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      intDoubleRow.set(indices[i] - startColInt, values[i]);
    }
  }

  /**
   * Add a element value without lock
   *
   * @param index element index
   * @param value element plus value
   */
  public void addTo(int index, double value) {
    set(index, get(index) + value);
  }

  /**
   * Add a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements plus values
   */
  public void addTo(int[] indices, double[] values) {
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
  private double[] getValues() {
    return intDoubleRow.getStorage().getValues();
  }

  /**
   * Get all element indices and values without lock, you must check the storage is sparse first use "isSparse";
   * if you want use original indices, you must plus with "startCol" first
   *
   * @return all element values
   */
  public ObjectIterator<Int2DoubleMap.Entry> getIter() {
    return intDoubleRow.getStorage().entryIterator();
  }

  @Override public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();
    try {
      switch (updateType) {
        case T_DOUBLE_SPARSE:
        case T_DOUBLE_SPARSE_COMPONENT:
          updateUseSparse(buf, op);
          break;

        case T_DOUBLE_DENSE:
        case T_DOUBLE_DENSE_COMPONENT:
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
        intDoubleRow.set(i, intDoubleRow.get(i) + buf.readDouble());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intDoubleRow.set(i, buf.readDouble());
      }
    }
  }

  private void updateUseSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        int index = buf.readInt();
        intDoubleRow.set(index, intDoubleRow.get(index) + buf.readDouble());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intDoubleRow.set(buf.readInt(), buf.readDouble());
      }
    }
  }


  @Override public int size() {
    return intDoubleRow.size();
  }

  /**
   * Merge this row split to a row
   *
   * @param mergedRow the dest row
   */
  public void mergeTo(IntDoubleVector mergedRow) {
    startRead();
    try {
      if (isDense()) {
        double[] values = getValues();
        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + startColInt, values[i]);
        }
      } else {
        ObjectIterator<Int2DoubleMap.Entry> iter = getIter();
        Int2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          mergedRow.set(entry.getIntKey() + startColInt, entry.getDoubleValue());
        }
      }
    } finally {
      endRead();
    }
  }

  @Override protected void serializeRow(ByteBuf buf) {
    if (useDenseSerialize()) {
      double[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        buf.writeDouble(values[i]);
      }
    } else {
      ObjectIterator<Int2DoubleMap.Entry> iter = getIter();
      Int2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeInt(entry.getIntKey());
        buf.writeDouble(entry.getDoubleValue());
      }
    }
  }

  @Override protected void deserializeRow(ByteBuf buf) {
    startColInt = (int) startCol;
    endColInt = (int) endCol;
    intDoubleRow = (IntDoubleVector) row;
    if (useDenseSerialize()) {
      for (int i = 0; i < size; i++) {
        intDoubleRow.set(i, buf.readDouble());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intDoubleRow.set(buf.readInt(), buf.readDouble());
      }
    }
  }

  @Override protected int getRowSpace() {
    if (useDenseSerialize()) {
      return 8 * size();
    } else {
      return 12 * size();
    }
  }

  @Override public ServerRow clone() {
    startRead();
    try {
      return new ServerIntDoubleRow(rowId, rowType, startColInt, endColInt, (int) estElemNum,
        intDoubleRow.clone());
    } finally {
      endRead();
    }
  }

  /**
   * Check the vector contains the index or not
   * @param index element index
   * @return true means exist
   */
  public boolean exist(int index) {
    if(intDoubleRow.isSparse()) {
      return intDoubleRow.getStorage().hasKey(index - startColInt);
    } else {
      return intDoubleRow.get(index - startColInt) != 0.0;
    }
  }

  public double initAndGet(int index, InitFunc func) {
    if(exist(index)) {
      return get(index);
    } else {
      double value = func.action();
      set(index, value);
      return value;
    }
  }

  @Override public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func)
    throws IOException {
    if(func != null) {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeDouble(initAndGet(in.readInt(), func));
        }
      } else {
        throw new IOException(this.getClass().getName() + " only support int type index now");
      }
    } else {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeDouble(get(in.readInt()));
        }
      } else {
        throw new IOException(this.getClass().getName() + " only support int type index now");
      }
    }
  }

  @Override public void setSplit(Vector row) {
    super.setSplit(row);
    intDoubleRow = (IntDoubleVector) row;
  }

  public void elemUpdate(DoubleElemUpdateFunc func) {
    if (isDense()) {
      double[] values = getValues();
      for(int i = 0; i < values.length; i++) {
        values[i] = func.update();
      }
    } else {
      ObjectIterator<Int2DoubleMap.Entry> iter = getIter();
      Int2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        entry.setValue(func.update());
      }
    }
  }
}
