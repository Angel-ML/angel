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
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.KeyType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.storage.vector.func.IntElemUpdateFunc;
import com.tencent.angel.ps.storage.vector.op.IIntIntOp;
import com.tencent.angel.ps.storage.vector.storage.IntIntStorage;
import com.tencent.angel.psagent.matrix.transport.router.RouterType;
import io.netty.buffer.ByteBuf;


/**
 * The row with "int" index type and "int" value type in PS
 */
public class ServerIntIntRow extends ServerBasicTypeRow implements IIntIntOp {

  /**
   * Create a new ServerIntDoubleRow
   *
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   * @param storage vector storage
   */
  public ServerIntIntRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum,
      IntIntStorage storage, RouterType routerType) {
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
  public ServerIntIntRow(int rowId, RowType rowType, int startCol, int endCol, int estElemNum, RouterType routerType) {
    this(rowId, rowType, startCol, endCol, estElemNum, null, routerType);
  }

  /**
   * Create a empty ServerIntIntRow
   *
   * @param rowType row type
   */
  public ServerIntIntRow(RowType rowType) {
    this(0, rowType, 0, 0, 0, RouterType.RANGE);
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  //Methods with out lock operation, you must call startWrite/startRead before using these methods
  // and call endWrite/endRead after
  //////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public IntIntStorage getStorage() {
    return (IntIntStorage) storage;
  }


  @Override
  public int get(int index) {
    return getStorage().get(index);
  }

  @Override
  public void set(int index, int value) {
    getStorage().set(index, value);
  }

  @Override
  public int[] get(int[] indices) {
    return getStorage().get(indices);
  }

  @Override
  public void set(int[] indices, int[] values) {
    assert indices.length == values.length;
    getStorage().set(indices, values);
  }

  @Override
  public void addTo(int index, int value) {
    getStorage().addTo(index, value);
  }

  @Override
  public void addTo(int[] indices, int[] values) {
    assert indices.length == values.length;
    getStorage().addTo(indices, values);
  }

  @Override
  public int size() {
    return getStorage().size();
  }

  @Override
  public void mergeTo(IntIntVector mergedRow) {
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
      return new ServerIntIntRow(rowId, rowType, (int) startCol, (int) endCol, (int) estElemNum,
          (IntIntStorage) storage.deepClone(), routerType);
    } finally {
      endRead();
    }
  }


  @Override
  public ServerRow adaptiveClone() {
    startRead();
    try {
      return new ServerIntIntRow(rowId, rowType, (int) startCol, (int) endCol, (int) estElemNum,
          (IntIntStorage) storage.adaptiveClone(), routerType);
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
  public int initAndGet(int index, InitFunc func) {
    return getStorage().initAndGet(index, func);
  }

  @Override
  public void indexGet(KeyType keyType, int indexSize, ByteBuf in, ByteBuf out, InitFunc func) {
    getStorage().indexGet(keyType, indexSize, in, out, func);
  }

  @Override
  public void elemUpdate(IntElemUpdateFunc func) {
    getStorage().elemUpdate(func);
  }
}
