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
import com.tencent.angel.ps.storage.vector.func.DoubleElemUpdateFunc;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.io.IOException;

/**
 * The row with "long" index type and "double" value type in PS
 */
public class ServerLongDoubleRow extends ServerDoubleRow {
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
    DoubleVector innerRow) {
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
    if(useIntKey) {
      return ((IntDoubleVector) row).get((int)(index - startCol));
    } else {
      return ((LongDoubleVector) row).get(index - startCol);
    }
  }

  /**
   * Set a element value without lock operation
   *
   * @param index element index
   * @param value element new value
   */
  public void set(long index, double value) {
    if(useIntKey) {
      ((IntDoubleVector) row).set((int)(index - startCol), value);
    } else {
      ((LongDoubleVector) row).set(index - startCol, value);
    }
  }

  /**
   * Get a batch elements values without lock
   *
   * @param indices elements indices
   * @return elements values
   */
  public double[] get(long[] indices) {
    double[] values = new double[indices.length];
    if(useIntKey) {
      for (int i = 0; i < indices.length; i++) {
        values[i] = ((IntDoubleVector) row).get((int)(indices[i] - startCol));
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        values[i] = ((LongDoubleVector) row).get(indices[i] - startCol);
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
  public void set(long[] indices, double[] values) {
    assert indices.length == values.length;
    if(useIntKey) {
      for (int i = 0; i < indices.length; i++) {
        ((IntDoubleVector) row).set((int)(indices[i] - startCol), values[i]);
      }
    } else {
      for (int i = 0; i < indices.length; i++) {
        ((LongDoubleVector) row).set(indices[i] - startCol, values[i]);
      }
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
  private double[] getValues() {
    if(useIntKey) {
      return ((IntDoubleVector) row).getStorage().getValues();
    } else {
      return ((LongDoubleVector) row).getStorage().getValues();
    }
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
      if(useIntKey) {
        for(int i = 0; i < size; i++) {
          int index = (int)buf.readLong();
          ((IntDoubleVector) row).set(index, ((IntDoubleVector) row).get(index) + buf.readDouble());
        }
      } else {
        for(int i = 0; i < size; i++) {
          long index = buf.readLong();
          ((LongDoubleVector) row).set(index, ((LongDoubleVector) row).get(index) + buf.readDouble());
        }
      }
    } else {
      if(useIntKey) {
        for(int i = 0; i < size; i++) {
          ((IntDoubleVector) row).set((int)buf.readLong(), buf.readDouble());
        }
      } else {
        for(int i = 0; i < size; i++) {
          ((LongDoubleVector) row).set(buf.readLong(), buf.readDouble());
        }
      }
    }
  }

  @Override public int size() {
    if(useIntKey) {
      return ((IntDoubleVector) row).size();
    } else {
      return (int)((LongDoubleVector) row).size();
    }
  }

  /**
   * Merge this row split to a row
   *
   * @param mergedRow the dest row
   */
  public void mergeTo(LongDoubleVector mergedRow) {
    startRead();
    try {
      if(isDense()) {
        double[] values = getValues();
        for (int i = 0; i < values.length; i++) {
          mergedRow.set(i + startCol, values[i]);
        }
      } else {
        if(useIntKey) {
          ObjectIterator<Int2DoubleMap.Entry> iter = ((IntDoubleVector) row).getStorage().entryIterator();
          Int2DoubleMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getIntKey() + startCol, entry.getDoubleValue());
          }
        } else {
          ObjectIterator<Long2DoubleMap.Entry> iter = ((LongDoubleVector) row).getStorage().entryIterator();
          Long2DoubleMap.Entry entry;
          while (iter.hasNext()) {
            entry = iter.next();
            mergedRow.set(entry.getLongKey() + startCol, entry.getDoubleValue());
          }
        }
      }
    } finally {
      endRead();
    }
  }

  @Override protected void serializeRow(ByteBuf buf) {
    if(isDense()) {
      double[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        buf.writeLong(i);
        buf.writeDouble(values[i]);
      }
    } else {
      if(useIntKey) {
        ObjectIterator<Int2DoubleMap.Entry> iter = ((IntDoubleVector) row).getStorage().entryIterator();
        Int2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          buf.writeLong(entry.getIntKey());
          buf.writeDouble(entry.getDoubleValue());
        }
      } else {
        ObjectIterator<Long2DoubleMap.Entry> iter = ((LongDoubleVector) row).getStorage().entryIterator();
        Long2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          buf.writeLong(entry.getLongKey());
          buf.writeDouble(entry.getDoubleValue());
        }
      }
    }
  }

  @Override protected void deserializeRow(ByteBuf buf) {
    if(useIntKey) {
      IntDoubleVector intDoubleRow = (IntDoubleVector) row;
      for (int i = 0; i < size; i++) {
        intDoubleRow.set((int)buf.readLong(), buf.readDouble());
      }
    } else {
      LongDoubleVector longDoubleRow = (LongDoubleVector) row;
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
      if(useIntKey) {
        return new ServerLongDoubleRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          ((IntDoubleVector) row).clone());
      } else {
        return new ServerLongDoubleRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          ((LongDoubleVector) row).clone());
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
      return ((IntDoubleVector) row).getStorage().hasKey((int)(index - startCol));
    } else {
      return ((LongDoubleVector) row).getStorage().hasKey(index - startCol);
    }
  }

  public double initAndGet(long index, InitFunc func) {
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
        for (int i = 0; i < indexSize; i++) {
          out.writeDouble(initAndGet(in.readLong(), func));
        }
      }
    } else {
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
  }

  //TODO
  public double defaultReturnValue() {
    return 0.0;
  }

  public void setDefaultValue(double defaultValue) {

  }

  @Override public void setSplit(Vector row) {
    super.setSplit(row);
  }

  @Override public void elemUpdate(DoubleElemUpdateFunc func) {
    if(isDense()) {
      double[] values = getValues();
      for (int i = 0; i < values.length; i++) {
        values[i] = func.update();
      }
    } else {
      if(useIntKey) {
        ObjectIterator<Int2DoubleMap.Entry> iter = ((IntDoubleVector) row).getStorage().entryIterator();
        Int2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          entry.setValue(func.update());
        }
      } else {
        ObjectIterator<Long2DoubleMap.Entry> iter = ((LongDoubleVector) row).getStorage().entryIterator();
        Long2DoubleMap.Entry entry;
        while (iter.hasNext()) {
          entry = iter.next();
          entry.setValue(func.update());
        }
      }
    }
  }
}
