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


package com.tencent.angel.ps.storage.matrix;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Server partition represents a partition of matrix, which hold and manager the related matrix rows.
 */
public class ServerPartition implements Serialize {
  private final static Log LOG = LogFactory.getLog(ServerPartition.class);

  /**
   * Row index to server row map
   */
  private final PartitionSource rows;

  /**
   * Partition key
   */
  private PartitionKey partitionKey;

  /**
   * Server Matrix row type
   */
  private RowType rowType;

  /**
   * Estimate sparsity for sparse model type
   */
  private final double estSparsity;

  private volatile int clock;

  private volatile PartitionState state;

  private final AtomicInteger updateCounter = new AtomicInteger(0);

  /**
   * Create a new Server partition,include load rows.
   *
   * @param partitionKey the partition meta
   * @param rowType      the row type
   */
  public ServerPartition(PartitionKey partitionKey, RowType rowType, double estSparsity,
    String sourceClass) {
    this.state = PartitionState.INITIALIZING;
    this.partitionKey = partitionKey;
    this.rowType = rowType;
    this.clock = 0;
    this.estSparsity = estSparsity;

    PartitionSource source;
    try {
      source = (PartitionSource) Class.forName(sourceClass).newInstance();
    } catch (Throwable e) {
      LOG.error("Can not init partition source for type " + sourceClass + " use default instead ",
        e);
      source = new PartitionSourceMap();
    }
    source.init(partitionKey.getEndRow() - partitionKey.getStartRow());
    this.rows = source;
  }

  /**
   * Create a new Server partition,include load rows.
   *
   * @param partitionKey the partition meta
   * @param rowType      the row type
   */
  public ServerPartition(PartitionKey partitionKey, RowType rowType, double estSparsity) {
    this(partitionKey, rowType, estSparsity, AngelConf.DEFAULT_ANGEL_PS_PARTITION_SOURCE_CLASS);
  }

  /**
   * Create a new Server partition.
   */
  public ServerPartition() {
    this(null, RowType.T_DOUBLE_DENSE, 1.0);
  }

  /**
   * Init partition
   */
  public void init() {
    if (partitionKey != null) {
      initRows(partitionKey, rowType, estSparsity);
    }
  }

  private void initRows(PartitionKey partitionKey, RowType rowType, double estSparsity) {
    int rowStart = partitionKey.getStartRow();
    int rowEnd = partitionKey.getEndRow();
    for (int rowIndex = rowStart; rowIndex < rowEnd; rowIndex++) {
      rows.putRow(rowIndex,
        initRow(rowIndex, rowType, partitionKey.getStartCol(), partitionKey.getEndCol(),
          estSparsity));
    }
  }

  private ServerRow initRow(int rowIndex, RowType rowType, long startCol, long endCol,
    double estSparsity) {
    int estEleNum = (int) ((endCol - startCol) * estSparsity);
    return ServerRowFactory.createServerRow(rowIndex, rowType, startCol, endCol, estEleNum);
  }

  private ServerRow initRow(RowType rowType) {
    return initRow(0, rowType, 0, 0, 0.0);
  }

  /**
   * Direct plus to rows in this partition
   *
   * @param buf serialized update vector
   */
  public void update(ByteBuf buf, UpdateOp op) {
    startUpdate();
    try {
      int rowNum = buf.readInt();
      int rowId;
      RowType rowType;

      for (int i = 0; i < rowNum; i++) {
        rowId = buf.readInt();
        rowType = RowType.valueOf(buf.readInt());
        ServerRow row = getRow(rowId);
        row.update(rowType, buf, op);
      }
    } finally {
      endUpdate();
    }
  }

  /**
   * Update the partition use psf
   *
   * @param func      PS update function
   * @param partParam the parameters for the PSF
   */
  public void update(UpdateFunc func, PartitionUpdateParam partParam) {
    startUpdate();
    try {
      func.partitionUpdate(partParam);
    } finally {
      endUpdate();
    }
  }

  /**
   * Replace the row in the partition
   *
   * @param rowSplit new row
   */
  public void update(ServerRow rowSplit) {
    ServerRow oldRowSplit = rows.getRow(rowSplit.getRowId());
    if (oldRowSplit == null || rowSplit.getClock() > oldRowSplit.getClock()
      || rowSplit.getRowVersion() > oldRowSplit.getRowVersion()) {
      rows.putRow(rowSplit.getRowId(), rowSplit);
    }
  }

  public void update(List<ServerRow> rowsSplit) {
    int size = rowsSplit.size();
    for (int i = 0; i < size; i++) {
      update(rowsSplit.get(i));
    }
  }

  /**
   * Gets specified row by row index.
   *
   * @param rowIndex the row index
   * @return the row
   */
  public ServerRow getRow(int rowIndex) {
    return rows.getRow(rowIndex);
  }

  /**
   * Get a batch rows
   *
   * @param rowIndexes row indices
   * @return
   */
  public List<ServerRow> getRows(List<Integer> rowIndexes) {
    if (rowIndexes == null || rowIndexes.isEmpty()) {
      return new ArrayList<>();
    }

    int size = rowIndexes.size();
    List<ServerRow> rows = new ArrayList<ServerRow>();
    for (int i = 0; i < size; i++) {
      rows.add(this.rows.getRow(i));
    }

    return rows;
  }

  public PartitionSource getRows() {
    return rows;
  }

  /**
   * Gets related partition key.
   *
   * @return the partition key
   */
  public PartitionKey getPartitionKey() {
    return partitionKey;
  }

  /**
   * Reset rows.
   */
  public void reset() {
    Iterator<Map.Entry<Integer, ServerRow>> iter = rows.iterator();
    while (iter.hasNext()) {
      ServerRow row = iter.next().getValue();
      row.reset();
    }
  }


  // TODO: dynamic add/delete row
  @Override public void serialize(ByteBuf buf) {
    if (partitionKey == null) {
      return;
    }

    partitionKey.serialize(buf);
    buf.writeInt(rowType.getNumber());
    buf.writeInt(rows.rowNum());

    Iterator<Map.Entry<Integer, ServerRow>> iter = rows.iterator();
    while (iter.hasNext()) {
      ServerRow row = iter.next().getValue();
      buf.writeInt(row.getRowType().getNumber());
      row.serialize(buf);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    partitionKey = new PartitionKey();
    partitionKey.deserialize(buf);
    rowType = RowType.valueOf(buf.readInt());
    int rowNum = buf.readInt();
    RowType rowType;
    for (int i = 0; i < rowNum; i++) {
      rowType = RowType.valueOf(buf.readInt());
      ServerRow row = initRow(rowType);
      row.deserialize(buf);
      rows.putRow(row.getRowId(), row);
    }
  }

  @Override public int bufferLen() {
    if (partitionKey == null) {
      return 0;
    }

    int len = partitionKey.bufferLen() + 8;

    Iterator<Map.Entry<Integer, ServerRow>> iter = rows.iterator();
    while (iter.hasNext()) {
      ServerRow row = iter.next().getValue();
      len += row.bufferLen();
    }

    return len;
  }

  public int elementNum() {
    int num = 0;

    Iterator<Map.Entry<Integer, ServerRow>> iter = rows.iterator();
    while (iter.hasNext()) {
      ServerRow row = iter.next().getValue();
      num = row.size();
    }

    return num;
  }

  public int getClock() {
    return clock;
  }

  public void setClock(int clock) {
    this.clock = clock;
  }

  public RowType getRowType() {
    return rowType;
  }

  public void waitAndSetReadOnly() throws InterruptedException {
    setState(PartitionState.READ_ONLY);
    while (true) {
      if (updateCounter.get() == 0) {
        return;
      } else {
        Thread.sleep(10);
      }
    }
  }

  public void setState(PartitionState state) {
    this.state = state;
  }

  public PartitionState getState() {
    return state;
  }

  public void startUpdate() {
    updateCounter.incrementAndGet();
  }

  public void endUpdate() {
    updateCounter.decrementAndGet();
  }

  public void recover(ServerPartition part) {
    startUpdate();
    try {
      Iterator<Map.Entry<Integer, ServerRow>> iter = part.rows.iterator();
      while (iter.hasNext()) {
        Map.Entry<Integer, ServerRow> entry = iter.next();
        rows.putRow(entry.getKey(), entry.getValue());
      }
      setState(PartitionState.READ_AND_WRITE);
    } finally {
      endUpdate();
    }
  }
}
