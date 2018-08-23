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

import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The row with "long" index type and "double" value type in PS
 */
public class ServerLongDoubleRow extends ServerRow {
  /**
   * Just a view of "row" in ServerRow
   */
  private LongDoubleVector longDoubleRow;

  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowId      row index
   * @param rowType    row type
   * @param startCol   start position
   * @param endCol     end position
   * @param estElemNum the estimate element number
   * @param innerRow   the inner row
   */
  public ServerLongDoubleRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
    LongDoubleVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
    this.longDoubleRow = (LongDoubleVector) row;
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
  public ServerLongDoubleRow(int rowId, RowType rowType, long startCol, long endCol,
    int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new ServerLongDoubleRow
   *
   * @param rowType
   */
  public ServerLongDoubleRow(RowType rowType) {
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
  public double get(long index) {
    return longDoubleRow.get(index - startCol);
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(long index, double value) {
    longDoubleRow.set(index - startCol, value);
  }

  /**
   * Get a batch elements values without lock
   *
   * @param indices elements indices
   * @return elements values
   */
  public double[] get(long[] indices) {
    double[] values = new double[indices.length];
    for (int i = 0; i < indices.length; i++) {
      values[i] = longDoubleRow.get(indices[i] - startCol);
    }
    return values;
  }

  /**
   * Set a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements values
   */
  public void set(long[] indices, double[] values) {
    assert indices.length == values.length;
    for (int i = 0; i < indices.length; i++) {
      longDoubleRow.set(indices[i] - startCol, values[i]);
    }
  }

  /**
   * Add a element value without lock
   *
   * @param index element index
   * @param value element plus value
   */
  public void addTo(long index, double value) {
    set(index, get(index) + value);
  }

  /**
   * Add a batch elements values without lock
   *
   * @param indices elements indices
   * @param values  elements plus values
   */
  public void addTo(long[] indices, double[] values) {
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
  public double[] getValues() {
    return longDoubleRow.getStorage().getValues();
  }

  /**
   * Get all element indices and values without lock, you must check the storage is sparse first use "isSparse";
   * if you want use original indices, you must plus with "startCol" first
   *
   * @return all element values
   */
  public ObjectIterator<Long2DoubleMap.Entry> getIter() {
    return longDoubleRow.getStorage().entryIterator();
  }


  @Override public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();
    try {
      switch (updateType) {
        case T_DOUBLE_SPARSE_LONGKEY:
        case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
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
    // TODO:default value
    buf.readDouble();
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        long index = buf.readLong();
        longDoubleRow.set(index, longDoubleRow.get(index) + buf.readDouble());
      }
    } else {
      for (int i = 0; i < size; i++) {
        longDoubleRow.set(buf.readLong(), buf.readDouble());
      }
    }
  }

  @Override public int size() {
    return (int) longDoubleRow.size();
  }

  /**
   * Merge this row split to a row
   *
   * @param mergedRow the dest row
   */
  public void mergeTo(LongDoubleVector mergedRow) {
    startRead();
    try {
      ObjectIterator<Long2DoubleMap.Entry> iter = getIter();
      Long2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        mergedRow.set(entry.getLongKey() + startCol, entry.getDoubleValue());
      }
    } finally {
      endRead();
    }
  }

  @Override protected void serializeRow(ByteBuf buf) {
    if (isSparse()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = getIter();
      Long2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        buf.writeLong(entry.getLongKey());
        buf.writeDouble(entry.getDoubleValue());
      }
    }
  }

  @Override protected void deserializeRow(ByteBuf buf) {
    longDoubleRow = (LongDoubleVector) row;
    if (isSparse()) {
      for (int i = 0; i < size; i++) {
        longDoubleRow.set(buf.readLong(), buf.readDouble());
      }
    }
  }

  @Override protected int getRowSpace() {
    return size() * 16;
  }

  @Override public ServerRow clone() {
    startRead();
    try {
      return new ServerLongDoubleRow(rowId, rowType, startCol, endCol, (int) estElemNum,
        longDoubleRow.clone());
    } finally {
      endRead();
    }
  }

  @Override protected void writeRow(DataOutputStream output) throws IOException {
    if (isSparse()) {
      output.writeInt(size());
      ObjectIterator<Long2DoubleMap.Entry> iter = getIter();
      Long2DoubleMap.Entry entry;
      while (iter.hasNext()) {
        entry = iter.next();
        output.writeLong(entry.getLongKey());
        output.writeDouble(entry.getDoubleValue());
      }
    }
  }

  @Override protected void readRow(DataInputStream input) throws IOException {
    longDoubleRow = (LongDoubleVector) row;
    if (isSparse()) {
      int size = input.readInt();
      for (int i = 0; i < size; i++) {
        longDoubleRow.set(input.readLong(), input.readDouble());
      }
    }
  }

  @Override public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out)
    throws IOException {
    if (indexType == IndexType.INT) {
      for (int i = 0; i < indexSize; i++) {
        out.writeDouble(get(in.readInt()));
      }
    } else {
      for (int i = 0; i < indexSize; i++) {
        out.writeDouble(get(in.readLong()));
      }
    }
  }

  //TODO
  public double defaultReturnValue() {
    return 0.0;
  }

  public void setDefaultValue(double defaultValue) {

  }

  @Override public void setSplit(Vector row) {
    super.setSplit(row);
    longDoubleRow = (LongDoubleVector) row;
  }
}
