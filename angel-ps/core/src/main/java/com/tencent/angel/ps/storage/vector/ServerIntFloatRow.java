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

import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.storage.vector.func.FloatElemUpdateFunc;
import com.tencent.angel.ps.storage.vector.op.IIntFloatOp;
import com.tencent.angel.ps.storage.vector.storage.IntFloatStorage;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The row with "int" index type and "float" value type in PS
 */
public class ServerIntFloatRow extends ServerBasicTypeRow implements IIntFloatOp {

  private static final Log LOG = LogFactory.getLog(ServerIntFloatRow.class);

  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   * @param storage inner storage
   */
  public ServerIntFloatRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum,
      IntFloatStorage storage) {
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
  public ServerIntFloatRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a empty ServerIntFloatRow
   *
   * @param rowType row type
   */
  public ServerIntFloatRow(RowType rowType) {
    this(0, rowType, 0, 0, 0);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  //Methods with out lock operation, you must call startWrite/startRead before using these methods
  // and call endWrite/endRead after
  //////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public IntFloatStorage getStorage() {
    return (IntFloatStorage) storage;
  }

  @Override
  public float get(int index) {
    return getStorage().get(index);
  }

  @Override
  public void set(int index, float value) {
    getStorage().set(index, value);
  }

  @Override
  public float[] get(int[] indices) {
    return getStorage().get(indices);
  }

  @Override
  public void set(int[] indices, float[] values) {
    assert indices.length == values.length;
    getStorage().set(indices, values);
  }

  @Override
  public void addTo(int index, float value) {
    getStorage().addTo(index, value);
  }

  @Override
  public void addTo(int[] indices, float[] values) {
    assert indices.length == values.length;
    getStorage().addTo(indices, values);
  }

  @Override
  public int size() {
    return getStorage().size();
  }

  @Override
  public void mergeTo(IntFloatVector mergedRow) {
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
      return new ServerIntFloatRow(rowId, rowType, (int) startCol, (int) endCol, (int) estElemNum,
          (IntFloatStorage) getStorage().deepClone());
    } finally {
      endRead();
    }
  }

  @Override
  public ServerRow adaptiveClone() {
    startRead();
    try {
      return new ServerIntFloatRow(rowId, rowType, (int) startCol, (int) endCol, (int) estElemNum,
          (IntFloatStorage) getStorage().adaptiveClone());
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
    return getStorage().exist(index);
  }

  @Override
  public float initAndGet(int index, InitFunc func) {
    return getStorage().initAndGet(index, func);
  }

  @Override
  public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    getStorage().indexGet(indexType, indexSize, in, out, func);
  }

  @Override
  public void elemUpdate(FloatElemUpdateFunc func) {
    getStorage().elemUpdate(func);
  }
}
