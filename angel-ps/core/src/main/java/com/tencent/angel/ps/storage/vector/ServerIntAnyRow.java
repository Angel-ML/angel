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

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.op.IIntElementOp;
import com.tencent.angel.ps.storage.vector.storage.IntElementStorage;

public class ServerIntAnyRow extends ServerComplexTypeRow implements IIntElementOp {

  /**
   * Create a new ServerIntAnyRow
   *
   * @param valueType element value type class
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   * @param storage vector storage
   */
  public ServerIntAnyRow(Class<? extends IElement> valueType, int rowId, RowType rowType,
      int startCol, int endCol, int estElemNum,
      IntElementStorage storage) {
    super(valueType, rowId, rowType, startCol, endCol, estElemNum, storage);
  }

  /**
   * Create a new ServerIntAnyRow
   *
   * @param valueType element value type class
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   */
  public ServerIntAnyRow(Class<? extends IElement> valueType, int rowId, RowType rowType,
      int startCol, int endCol, int estElemNum) {
    this(valueType, rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new empty ServerIntAnyRow
   */
  public ServerIntAnyRow(RowType rowType) {
    this(null, 0, rowType, 0, 0, 0, null);
  }

  @Override
  public IntElementStorage getStorage() {
    return (IntElementStorage) storage;
  }

  @Override
  public int size() {
    return getStorage().size();
  }

  @Override
  public ServerIntAnyRow adaptiveClone() {
    return new ServerIntAnyRow(valueType, rowId, rowType, (int) startCol, (int) endCol,
        (int) estElemNum,
        (IntElementStorage) getStorage().adaptiveClone());
  }

  @Override
  public ServerIntAnyRow deepClone() {
    return new ServerIntAnyRow(valueType, rowId, rowType, (int) startCol, (int) endCol,
        (int) estElemNum,
        (IntElementStorage) getStorage().deepClone());
  }

  @Override
  public IElement get(int index) {
    return getStorage().get(index);
  }

  @Override
  public void set(int index, IElement value) {
    getStorage().set(index, value);
  }

  @Override
  public IElement[] get(int[] indices) {
    return getStorage().get(indices);
  }

  @Override
  public void set(int[] indices, IElement[] values) {
    getStorage().set(indices, values);
  }

  @Override
  public boolean exist(int index) {
    return getStorage().exist(index);
  }
}
