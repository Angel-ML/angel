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


package com.tencent.angel.ps.storage.partition;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.utils.RowType;

import com.tencent.angel.ps.storage.partition.op.IServerRowsStorageOp;
import com.tencent.angel.ps.storage.partition.storage.ServerRowsStorage;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Row-based matrix partition
 */
public class RowBasedPartition extends ServerPartition implements IServerRowsStorageOp {

  private final static Log LOG = LogFactory.getLog(RowBasedPartition.class);

  /**
   * Row element value type
   */
  private transient Class<? extends IElement> valueClass;

  /**
   * Create new RowBasedPartition
   *
   * @param partKey partition key
   * @param estSparsity estimate sparsity
   * @param storage row-based matrix partition storage
   * @param rowType row type
   * @param valueClass row element value type
   */
  public RowBasedPartition(PartitionKey partKey, double estSparsity, ServerRowsStorage storage,
      RowType rowType, Class<? extends IElement> valueClass) {
    super(partKey, rowType, estSparsity, storage);
    this.valueClass = valueClass;
  }

  /**
   * Create a new Server partition.
   */
  public RowBasedPartition() {
    this(null, 1.0, null, RowType.T_DOUBLE_DENSE, null);
  }

  @Override
  public void init() {
    getRowsStorage().init(partKey, rowType, estSparsity, valueClass);
  }

  /**
   * Put the server row to the partition
   *
   * @param rowSplit server row
   */
  public void putRow(ServerRow rowSplit) {
    putRow(rowSplit.getRowId(), rowSplit);
  }

  @Override
  public void putRow(int rowId, ServerRow rowSplit) {
    ServerRow oldRowSplit = getRow(rowSplit.getRowId());
    if (oldRowSplit == null || rowSplit.getClock() > oldRowSplit.getClock()
        || rowSplit.getRowVersion() > oldRowSplit.getRowVersion()) {
      putRow(rowSplit.getRowId(), rowSplit);
    }
  }

  /**
   * Put the server rows to the partition
   *
   * @param rowsSplit server rows
   */
  public void putRows(List<ServerRow> rowsSplit) {
    int size = rowsSplit.size();
    for (int i = 0; i < size; i++) {
      putRow(rowsSplit.get(i).getRowId(), rowsSplit.get(i));
    }
  }

  @Override
  public void putRows(List<Integer> rowIds, List<ServerRow> rowsSplit) {
    assert rowIds.size() == rowsSplit.size();
    int size = rowIds.size();
    for (int i = 0; i < size; i++) {
      putRow(rowIds.get(i), rowsSplit.get(i));
    }
  }

  @Override
  public int getRowNum() {
    return getRowsStorage().getRowNum();
  }

  @Override
  public void reset() {
    getStorage().reset();
  }

  @Override
  public ServerRow getRow(int rowId) {
    return getRowsStorage().getRow(rowId);
  }

  @Override
  public List<ServerRow> getRows(List<Integer> rowIds) {
    return getRowsStorage().getRows(rowIds);
  }

  @Override
  public boolean hasRow(int rowId) {
    return getRowsStorage().hasRow(rowId);
  }

  @Override
  public Iterator<Entry<Integer, ServerRow>> iterator() {
    return getRowsStorage().iterator();
  }

  public ServerRowsStorage getRowsStorage() {
    return (ServerRowsStorage) getStorage();
  }

  public Class<? extends IElement> getValueClass() {
    return valueClass;
  }
}
