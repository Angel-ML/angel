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

import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.storage.vector.func.LongElemUpdateFunc;
import com.tencent.angel.ps.storage.vector.op.ILongLongOp;
import com.tencent.angel.ps.storage.vector.storage.LongLongStorage;
import io.netty.buffer.ByteBuf;

/**
 * The row with "long" index type and "long" value type in PS
 */
public class ServerLongLongRow extends ServerBasicTypeRow implements ILongLongOp {

  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   */
  public ServerLongLongRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
      LongLongStorage storage) {
    super(rowId, rowType, startCol, endCol, estElemNum, storage);
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
  public ServerLongLongRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum) {
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

  @Override
  public LongLongStorage getStorage() {
    return (LongLongStorage) storage;
  }

  @Override
  public long get(long index) {
    return getStorage().get(index);
  }

  @Override
  public void set(long index, long value) {
    getStorage().set(index, value);
  }

  @Override
  public long[] get(long[] indices) {
    return getStorage().get(indices);
  }

  @Override
  public void set(long[] indices, long[] values) {
    assert indices.length == values.length;
    getStorage().set(indices, values);
  }

  @Override
  public void addTo(long index, long value) {
    getStorage().addTo(index, value);
  }

  @Override
  public void addTo(long[] indices, long[] values) {
    assert indices.length == values.length;
    getStorage().addTo(indices, values);
  }

  @Override
  public int size() {
    return getStorage().size();
  }

  @Override
  public void mergeTo(LongLongVector mergedRow) {
    startRead();
    try {
      getStorage().mergeTo(mergedRow);
    } finally {
      endRead();
    }
  }

  @Override
  public ServerRow deepClone() {
    startRead();
    try {
      return new ServerLongLongRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          (LongLongStorage) storage.deepClone());
    } finally {
      endRead();
    }
  }

  @Override
  public ServerRow
  adaptiveClone() {
    startRead();
    try {
      return new ServerLongLongRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          (LongLongStorage) storage.adaptiveClone());
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
    return getStorage().exist(index);
  }

  @Override
  public long initAndGet(long index, InitFunc func) {
    return getStorage().initAndGet(index, func);
  }

  @Override
  public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    getStorage().indexGet(indexType, indexSize, in, out, func);
  }

  @Override
  public void elemUpdate(LongElemUpdateFunc func) {
    getStorage().elemUpdate(func);
  }
}
