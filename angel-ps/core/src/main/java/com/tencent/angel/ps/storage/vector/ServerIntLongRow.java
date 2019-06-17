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

import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.func.LongElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   * @param innerRow inner row
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
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
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
   * @param values elements values
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
   * @param values elements plus values
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
   * Get all element indices and values without lock, you must check the storage is sparse first use
   * "isSparse"; if you want use original indices, you must plus with "startCol" first
   *
   * @return all element values
   */
  public ObjectIterator<Int2LongMap.Entry> getIter() {
    return intLongRow.getStorage().entryIterator();
  }


  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();
    try {
      switch (updateType) {
        case T_LONG_SPARSE:
        case T_LONG_SPARSE_COMPONENT:
          updateUseIntLongSparse(buf, op);
          break;

        case T_INT_SPARSE:
        case T_INT_SPARSE_COMPONENT:
          updateUseIntIntSparse(buf, op);
          break;

        case T_LONG_DENSE:
        case T_LONG_DENSE_COMPONENT:
          updateUseIntLongDense(buf, op);
          break;

        case T_INT_DENSE:
        case T_INT_DENSE_COMPONENT:
          updateUseIntIntDense(buf, op);
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

  private void updateUseIntLongDense(ByteBuf buf, UpdateOp op) {
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

  private void updateUseIntIntDense(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        intLongRow.set(i, intLongRow.get(i) + buf.readInt());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intLongRow.set(i, buf.readInt());
      }
    }
  }

  private void updateUseIntLongSparse(ByteBuf buf, UpdateOp op) {
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


  private void updateUseIntIntSparse(ByteBuf buf, UpdateOp op) {
    int size = buf.readInt();
    if (op == UpdateOp.PLUS) {
      for (int i = 0; i < size; i++) {
        int index = buf.readInt();
        intLongRow.set(index, intLongRow.get(index) + buf.readInt());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intLongRow.set(buf.readInt(), buf.readInt());
      }
    }
  }

  @Override
  public int size() {
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

  @Override
  protected void serializeRow(ByteBuf buf) {
    if (useDenseSerialize()) {
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

  @Override
  protected void deserializeRow(ByteBuf buf) {
    startColInt = (int) startCol;
    endColInt = (int) endCol;
    intLongRow = (IntLongVector) row;

    if (useDenseSerialize()) {
      for (int i = 0; i < size; i++) {
        intLongRow.set(i, buf.readLong());
      }
    } else {
      for (int i = 0; i < size; i++) {
        intLongRow.set(buf.readInt(), buf.readLong());
      }
    }
  }

  @Override
  public int getRowSpace() {
    if (useDenseSerialize()) {
      return 8 * size();
    } else {
      return 12 * size();
    }
  }

  @Override
  public ServerRow clone() {
    startRead();
    try {
      return new ServerIntLongRow(rowId, rowType, startColInt, endColInt, (int) estElemNum,
          intLongRow.clone());
    } finally {
      endRead();
    }
  }

  @Override
  public ServerRow
adaptiveClone() {
    startRead();
    try {
      if (intLongRow.isSparse()) {
        return new ServerIntLongRow(rowId, rowType, startColInt, endColInt, (int) estElemNum,
            VFactory.sortedLongVector(endColInt - startColInt, intLongRow.getStorage().getIndices(),
                intLongRow.getStorage().getValues()));
      } else {
        return new ServerIntLongRow(rowId, rowType, startColInt, endColInt, (int) estElemNum,
            intLongRow);
      }

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
    if (intLongRow.isSparse()) {
      return intLongRow.getStorage().hasKey(index - startColInt);
    } else {
      return intLongRow.get(index - startColInt) != 0;
    }
  }

  public long initAndGet(int index, InitFunc func) {
    if (exist(index)) {
      return get(index);
    } else {
      long value = (long) func.action();
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
          out.writeLong(initAndGet(in.readInt(), func));
        }
      } else {
        throw new IOException(this.getClass().getName() + " only support int type index now");
      }
    } else {
      if (indexType == IndexType.INT) {
        for (int i = 0; i < indexSize; i++) {
          out.writeLong(get(in.readInt()));
        }
      } else {
        throw new IOException(this.getClass().getName() + " only support int type index now");
      }
    }
  }

  @Override
  public void setSplit(Vector row) {
    super.setSplit(row);
    intLongRow = (IntLongVector) row;
  }

  @Override
  public void elemUpdate(LongElemUpdateFunc func) {
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
