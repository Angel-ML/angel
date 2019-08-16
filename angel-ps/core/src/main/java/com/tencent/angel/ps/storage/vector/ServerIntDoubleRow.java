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
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.storage.vector.func.DoubleElemUpdateFunc;
import com.tencent.angel.ps.storage.vector.op.IIntDoubleOp;
import com.tencent.angel.ps.storage.vector.storage.IntDoubleStorage;
import io.netty.buffer.ByteBuf;

/**
 * The row with "int" index type and "double" value type in PS
 */
public class ServerIntDoubleRow extends ServerBasicTypeRow implements IIntDoubleOp {

  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   * @param storage data storage
   */
  private ServerIntDoubleRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum,
      IntDoubleStorage storage) {
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

  @Override
  public IntDoubleStorage getStorage() {
    return (IntDoubleStorage) storage;
  }

  @Override
  public double get(int index) {
    return getStorage().get(index);
  }

  @Override
  public void set(int index, double value) {
    getStorage().set(index, value);
  }

  @Override
  public double[] get(int[] indices) {
    return getStorage().get(indices);
  }

  @Override
  public void set(int[] indices, double[] values) {
    assert indices.length == values.length;
    getStorage().set(indices, values);
  }

  @Override
  public void addTo(int index, double value) {
    getStorage().addTo(index, value);
  }

  @Override
  public void addTo(int[] indices, double[] values) {
    assert indices.length == values.length;
    getStorage().addTo(indices, values);
  }

  @Override
  public int size() {
    return getStorage().size();
  }

  @Override
  public void mergeTo(IntDoubleVector mergedRow) {
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
      return new ServerIntDoubleRow(rowId, rowType, (int) startCol, (int) endCol, (int) estElemNum,
          (IntDoubleStorage) storage.deepClone());
    } finally {
      endRead();
    }
  }

  @Override
  public ServerRow adaptiveClone() {
    startRead();
    try {
      return new ServerIntDoubleRow(rowId, rowType, (int) startCol, (int) endCol, (int) estElemNum,
          (IntDoubleStorage) storage.adaptiveClone());
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
  public double initAndGet(int index, InitFunc func) {
    return getStorage().initAndGet(index, func);
  }

  @Override
  public void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    getStorage().indexGet(indexType, indexSize, in, out, func);
  }

  @Override
  public void elemUpdate(DoubleElemUpdateFunc func) {
    getStorage().elemUpdate(func);
  }
}
