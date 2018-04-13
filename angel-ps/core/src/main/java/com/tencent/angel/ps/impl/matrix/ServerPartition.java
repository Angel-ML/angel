/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ps.impl.matrix;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.model.output.format.ModelPartitionMeta;
import com.tencent.angel.model.output.format.ModelPartitionMeta.RowOffset;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * The Server partition represents a partition of matrix, which hold and manager the related matrix rows.
 */
public class ServerPartition implements Serialize {
  private final static Log LOG = LogFactory.getLog(ServerPartition.class);

  private final ConcurrentHashMap<Integer, ServerRow> rows;

  private PartitionKey partitionKey;

  private RowType rowType;

  private int clock;

  private volatile PartitionState state;

  private final AtomicInteger updateCounter = new AtomicInteger(0);

  private double estSparsity;

  /**
   * Create a new Server partition,include load rows.
   *
   * @param partitionKey the partition meta
   * @param rowType      the row type
   */
  public ServerPartition(PartitionKey partitionKey, RowType rowType, double estSparsity) {
    this.state = PartitionState.INITIALIZING;
    this.partitionKey = partitionKey;
    this.rowType = rowType;
    this.rows = new ConcurrentHashMap<>();
    this.clock = 0;
    this.estSparsity = estSparsity;
  }

  /**
   * Init partition
   */
  public void init() {
    if (partitionKey != null) {
      initRows(partitionKey, rowType, estSparsity);
    }
  }

  /**
   * Create a new Server partition.
   */
  public ServerPartition() {
    this(null, RowType.T_DOUBLE_DENSE, 1.0);
  }

  /**
   * Gets specified row by row index.
   *
   * @param rowIndex the row index
   * @return the row
   */
  public ServerRow getRow(int rowIndex) {
    return rows.get(rowIndex);
  }

  /**
   * Gets related partition key.
   *
   * @return the partition key
   */
  public PartitionKey getPartitionKey() {
    return partitionKey;
  }

  private void initRows(PartitionKey partitionKey, RowType rowType, double estSparsity) {
    int rowStart = partitionKey.getStartRow();
    int rowEnd = partitionKey.getEndRow();
    for (int rowIndex = rowStart; rowIndex < rowEnd; rowIndex++) {
      rows.put(rowIndex, initRow(rowIndex, rowType, partitionKey.getStartCol(), partitionKey.getEndCol(), estSparsity));
    }
  }

  private ServerRow initRow(int rowIndex, RowType rowType, long startCol, long endCol, double estSparsity) {
    int estEleNum = (int)((endCol - startCol) * estSparsity);
    switch (rowType) {
      case T_DOUBLE_DENSE:
        return new ServerDenseDoubleRow(rowIndex, (int)startCol, (int)endCol);

      case T_FLOAT_DENSE:
        return new ServerDenseFloatRow(rowIndex, (int)startCol, (int)endCol);

      case T_DOUBLE_SPARSE:
      case T_DOUBLE_SPARSE_COMPONENT:
        return new ServerSparseDoubleRow(rowIndex, (int)startCol, (int)endCol, estEleNum);

      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return new ServerSparseDoubleLongKeyRow(rowIndex, startCol, endCol, estEleNum);

      case T_INT_DENSE:
        return new ServerDenseIntRow(rowIndex, (int)startCol, (int)endCol);

      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        return new ServerSparseIntRow(rowIndex, (int)startCol, (int)endCol, estEleNum);

      case T_INT_ARBITRARY:
        return new ServerArbitraryIntRow(rowIndex, (int)startCol, (int)endCol);

      case T_FLOAT_SPARSE:
      case T_FLOAT_SPARSE_COMPONENT:
        return  new ServerSparseFloatRow(rowIndex, (int)startCol, (int)endCol, estEleNum);

      default:
        LOG.warn("invalid rowtype " + rowType + ", default is " + RowType.T_DOUBLE_DENSE);
        return new ServerDenseDoubleRow(rowIndex, (int)startCol, (int)endCol);
    }
  }

  private ServerRow initRow(RowType rowType) {
    switch (rowType) {
      case T_DOUBLE_DENSE:
        return new ServerDenseDoubleRow();

      case T_DOUBLE_SPARSE:
      case T_DOUBLE_SPARSE_COMPONENT:
        return new ServerSparseDoubleRow();

      case T_DOUBLE_SPARSE_LONGKEY:
      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        return new ServerSparseDoubleLongKeyRow();

      case T_INT_DENSE:
        return new ServerDenseIntRow();

      case T_FLOAT_DENSE:
        return new ServerDenseFloatRow();

      case T_FLOAT_SPARSE:
      case T_FLOAT_SPARSE_COMPONENT:
        return new ServerSparseFloatRow();

      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        return new ServerSparseIntRow();

      case T_INT_ARBITRARY:
        return new ServerArbitraryIntRow();

      default:
        LOG.warn("invalid rowtype " + rowType + ", default is " + RowType.T_DOUBLE_DENSE);
        return new ServerDenseDoubleRow();
    }
  }

  /**
   * Reset rows.
   */
  public void reset() {
    for(ServerRow row : rows.values()) {
      row.reset();
    }
  }

  /**
   * Save a matrix partition to file.
   *
   * @param output the output
   * @throws IOException the io exception
   */
  public void save(DataOutputStream output) throws IOException {
    save(output, null);
  }


  /**
   * Save a matrix partition to file.
   *
   * @param output the output
   * @param partitionMeta the meta
   * @throws IOException the io exception
   */
  public void save(DataOutputStream output , ModelPartitionMeta partitionMeta) throws IOException {
    FSDataOutputStream dataOutputStream = new FSDataOutputStream(output, null,
        partitionMeta != null ? partitionMeta.getOffset() : 0);
    dataOutputStream.writeInt(rows.size());
    long offset;
    for (Map.Entry<Integer, ServerRow> entry : rows.entrySet()) {
      offset = dataOutputStream.getPos();
      dataOutputStream.writeInt(entry.getKey());
      ServerRow row = entry.getValue();
      row.writeTo(dataOutputStream);
      if (partitionMeta != null) {
        partitionMeta.setRowMeta(new RowOffset(entry.getKey(), offset));
      }
    }
  }

  /**
   * Load partition from model file.
   *
   * @param input the input
   * @throws IOException
   */
  public void load(DataInputStream input) throws IOException {
    try {
      int size = input.readInt();
      for (int i = 0; i < size; i ++) {
        int rowId = input.readInt();
        ServerRow serverRow = rows.get(rowId);
        serverRow.readFrom(input);
      }
    } finally {
      setState(PartitionState.READ_AND_WRITE);
    }
  }

  @Override
  public void serialize(ByteBuf buf) {
    if (partitionKey == null) {
      return;
    }

    partitionKey.serialize(buf);
    buf.writeInt(rowType.getNumber());
    buf.writeInt(rows.size());
    for (java.util.Map.Entry<Integer, ServerRow> rowEntry : rows.entrySet()) {
      buf.writeInt(rowEntry.getValue().getRowType().getNumber());
      rowEntry.getValue().serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    partitionKey = new PartitionKey();
    partitionKey.deserialize(buf);
    rowType = RowType.valueOf(buf.readInt());
    int rowNum = buf.readInt();
    RowType rowType;
    for (int i = 0; i < rowNum; i++) {
      rowType = RowType.valueOf(buf.readInt());
      ServerRow row = initRow(rowType);
      row.deserialize(buf);
      rows.put(row.getRowId(), row);
    }
  }

  @Override
  public int bufferLen() {
    if (partitionKey == null) {
      return 0;
    }

    int len = partitionKey.bufferLen() + 8;
    for (java.util.Map.Entry<Integer, ServerRow> rowEntry : rows.entrySet()) {
      len += rowEntry.getValue().bufferLen();
    }

    return len;
  }

  public List<ServerRow> getRows(List<Integer> rowIndexes) {
    if (rowIndexes == null || rowIndexes.isEmpty()) {
      return new ArrayList<ServerRow>();
    }

    int size = rowIndexes.size();
    List<ServerRow> rows = new ArrayList<ServerRow>();
    for (int i = 0; i < size; i++) {
      rows.add(this.rows.get(i));
    }

    return rows;
  }

  public void update(ServerRow rowSplit) {
    ServerRow oldRowSplit = rows.get(rowSplit.getRowId());
    if(oldRowSplit == null || rowSplit.clock > oldRowSplit.clock || rowSplit.rowVersion > oldRowSplit.rowVersion){
      rows.put(rowSplit.getRowId(), rowSplit);
    }
  }

  public void update(List<ServerRow> rowsSplit) {
    int size = rowsSplit.size();
    for(int i = 0; i < size; i++){
      update(rowsSplit.get(i));
    }
  }

  public int elementNum() {
    int num = 0;
    for(ServerRow row:rows.values()) {
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
    while(true) {
      if(updateCounter.get() == 0) {
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
      rows.putAll(part.rows);
      setState(PartitionState.READ_AND_WRITE);
    } finally {
      endUpdate();
    }
  }

  public void update(ByteBuf buf, RowUpdater updater) throws Exception {
    startUpdate();
    try {
      int rowNum = buf.readInt();
      int rowId;
      RowType rowType;
      int size;
      for (int i = 0; i < rowNum; i++) {
        rowId = buf.readInt();
        rowType = RowType.valueOf(buf.readInt());
        ServerRow row = getRow(rowId);
        updater.update(rowType, buf, row);
      }
    } finally {
      endUpdate();
    }
  }

  public void update(UpdateFunc func, PartitionUpdateParam partParam) {
    startUpdate();
    try {
      func.partitionUpdate(partParam);
    } finally {
      endUpdate();
    }
  }
}
