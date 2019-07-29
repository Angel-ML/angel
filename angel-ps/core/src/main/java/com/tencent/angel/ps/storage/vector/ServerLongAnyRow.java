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
import com.tencent.angel.ps.storage.vector.op.ILongElementOp;
import com.tencent.angel.ps.storage.vector.storage.LongElementStorage;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class ServerLongAnyRow extends ServerComplexTypeRow implements ILongElementOp {

  /**
   * Create a new ServerLongAnyRow
   *
   * @param valueType element value type class
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   * @param storage vector storage
   */
  public ServerLongAnyRow(Class<? extends IElement> valueType, int rowId, RowType rowType,
      long startCol, long endCol, long estElemNum,
      LongElementStorage storage) {
    super(valueType, rowId, rowType, startCol, endCol, estElemNum, storage);
  }

  /**
   * Create a new ServerLongAnyRow
   *
   * @param valueType element value type class
   * @param rowId row index
   * @param rowType row type
   * @param startCol start position
   * @param endCol end position
   * @param estElemNum the estimate element number
   */
  public ServerLongAnyRow(Class<? extends IElement> valueType, int rowId, RowType rowType,
      long startCol, long endCol, long estElemNum) {
    this(valueType, rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new empty ServerLongAnyRow
   */
  public ServerLongAnyRow(RowType rowType) {
    this(null, 0, rowType, 0, 0, 0, null);
  }

  @Override
  public LongElementStorage getStorage() {
    return (LongElementStorage) storage;
  }

  @Override
  public int size() {
    return getStorage().size();
  }

  public ObjectIterator<Long2ObjectMap.Entry<IElement>> iterator() {
    return this.getStorage().iterator();
  }

  @Override
  public ServerRow adaptiveClone() {
    return new ServerLongAnyRow(valueType, rowId, rowType, startCol, endCol, estElemNum,
        (LongElementStorage) getStorage().adaptiveClone());
  }

  @Override
  public ServerRow deepClone() {
    return new ServerLongAnyRow(valueType, rowId, rowType, startCol, endCol, estElemNum,
        (LongElementStorage) getStorage().deepClone());
  }

  @Override
  public IElement get(long index) {
    return getStorage().get(index);
  }

  @Override
  public void set(long index, IElement value) {
    getStorage().set(index, value);
  }

  @Override
  public IElement[] get(long[] indices) {
    return getStorage().get(indices);
  }

  @Override
  public void set(long[] indices, IElement[] values) {
    getStorage().set(indices, values);
  }

  @Override
  public boolean exist(long index) {
    return getStorage().exist(index);
  }
}
