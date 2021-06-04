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
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.storage.vector.func.FloatElemUpdateFunc;
import com.tencent.angel.ps.storage.vector.op.ILongFloatOp;
import com.tencent.angel.ps.storage.vector.storage.LongFloatStorage;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;
import io.netty.buffer.ByteBuf;

/**
 * The row with "long" index type and "float" value type in PS
 */
public class ServerLongFloatRow extends ServerBasicTypeRow implements ILongFloatOp {

  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   */
  public ServerLongFloatRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
      LongFloatStorage storage, RouterType routerType) {
    super(rowId, rowType, startCol, endCol, estElemNum, storage, routerType);
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
  public ServerLongFloatRow(int rowId, RowType rowType, long startCol, long endCol,
      int estElemNum, RouterType routerType) {
    this(rowId, rowType, startCol, endCol, estElemNum, null, routerType);
  }

  /**
   * Create a new ServerLongFloatRow
   *
   * @param rowType row type
   */
  public ServerLongFloatRow(RowType rowType) {
    this(0, rowType, 0, 0, 0, RouterType.RANGE);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  //Methods with out lock operation, you must call startWrite/startRead before using these methods
  // and call endWrite/endRead after
  //////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public LongFloatStorage getStorage() {
    return (LongFloatStorage) storage;
  }

  @Override
  public float get(long index) {
    return getStorage().get(index);
  }

  @Override
  public void set(long index, float value) {
    getStorage().set(index, value);
  }

  @Override
  public float[] get(long[] indices) {
    return getStorage().get(indices);
  }

  @Override
  public void set(long[] indices, float[] values) {
    assert indices.length == values.length;
    getStorage().set(indices, values);
  }

  @Override
  public void addTo(long index, float value) {
    getStorage().addTo(index, value);
  }

  @Override
  public void addTo(long[] indices, float[] values) {
    assert indices.length == values.length;
    getStorage().addTo(indices, values);
  }

  @Override
  public int size() {
    return getStorage().size();
  }

  @Override
  public void mergeTo(LongFloatVector mergedRow) {
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
      return new ServerLongFloatRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          (LongFloatStorage) storage.deepClone(), routerType);
    } finally {
      endRead();
    }
  }

  @Override
  public ServerRow adaptiveClone() {
    startRead();
    try {
      return new ServerLongFloatRow(rowId, rowType, startCol, endCol, (int) estElemNum,
          (LongFloatStorage) storage.adaptiveClone(), routerType);
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
  public float initAndGet(long index, InitFunc func) {
    return getStorage().initAndGet(index, func);
  }

  @Override
  public void indexGet(KeyType keyType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    getStorage().indexGet(keyType, indexSize, in, out, func);
  }

  @Override
  public void elemUpdate(FloatElemUpdateFunc func) {
    getStorage().elemUpdate(func);
  }
}
