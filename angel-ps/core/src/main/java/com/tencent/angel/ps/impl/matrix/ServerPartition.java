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
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The Server partition represents a partition of matrix, which hold and manager the related matrix rows.
 */
public class ServerPartition implements Serialize {
  private final static Log LOG = LogFactory.getLog(ServerPartition.class);

  private final ConcurrentHashMap<Integer, ServerRow> rows;
  private final Int2IntOpenHashMap taskIndexToClockMap;
  private final ReentrantReadWriteLock lock;

  private RowType rowType;
  protected PartitionKey partitionKey;
  protected int minClock;

  /**
   * Create a new Server partition,include load rows.
   *
   * @param partitionKey the partition key
   * @param rowType      the row type
   */
  public ServerPartition(PartitionKey partitionKey, RowType rowType) {
    this.partitionKey = partitionKey;
    this.rowType = rowType;
    this.minClock = 0;

    this.taskIndexToClockMap = new Int2IntOpenHashMap();
    this.lock = new ReentrantReadWriteLock();
    this.rows = new ConcurrentHashMap<Integer, ServerRow>();
    if (partitionKey != null) {
      initRows(partitionKey, rowType);
    }
  }

  /**
   * Create a new Server partition.
   */
  public ServerPartition() {
    this(null, RowType.T_INVALID);
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

  /**
   * Clock of task
   *
   * @param taskIndex the task index
   * @param clock     the clock
   */
  public void clock(int taskIndex, int clock) {
    try {
      lock.writeLock().lock();
      if (!taskIndexToClockMap.containsKey(taskIndex)) {
        taskIndexToClockMap.put(taskIndex, clock);
      } else {
        int oldClock = taskIndexToClockMap.get(taskIndex);
        if (oldClock < clock) {
          taskIndexToClockMap.put(taskIndex, clock);
        }
      }

      if (minClock < clock) {
        refreshMinClock();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void refreshMinClock() {
    if (taskIndexToClockMap.size() < PSContext.get().getTaskNum()) {
      minClock = 0;
      return;
    }

    int min = Integer.MAX_VALUE;
    for (Entry entry : taskIndexToClockMap.int2IntEntrySet()) {
      if (entry.getIntValue() < min) {
        min = entry.getIntValue();
      }
    }

    if (minClock < min) {
      minClock = min;
      for (ServerRow row : rows.values()) {
        row.setClock(min);
      }
    }
  }

  /**
   * Gets represented clock.
   *
   * @return the clock
   */
  public int getClock() {
    try {
      lock.readLock().lock();
      return minClock;
    } finally {
      lock.readLock().unlock();
    }
  }

  private void initRows(PartitionKey partitionKey, RowType rowType) {
    int rowStart = partitionKey.getStartRow();
    int rowEnd = partitionKey.getEndRow();
    for (int rowIndex = rowStart; rowIndex < rowEnd; rowIndex++) {
      rows.put(rowIndex, initRow(partitionKey, rowIndex, rowType));
    }
  }

  private ServerRow initRow(PartitionKey partitionKey, int rowIndex, RowType rowType) {
    switch (rowType) {
      case T_DOUBLE_DENSE:
        return new ServerDenseDoubleRow(rowIndex, partitionKey.getStartCol(),
            partitionKey.getEndCol());
      case T_FLOAT_DENSE:
        return new ServerDenseFloatRow(rowIndex, partitionKey.getStartCol(),
            partitionKey.getEndCol());

      case T_DOUBLE_SPARSE:
        return new ServerSparseDoubleRow(rowIndex, partitionKey.getStartCol(),
            partitionKey.getEndCol());

      case T_INT_DENSE:
        return new ServerDenseIntRow(rowIndex, partitionKey.getStartCol(), partitionKey.getEndCol());

      case T_INT_SPARSE:
        return new ServerSparseIntRow(rowIndex, partitionKey.getStartCol(),
            partitionKey.getEndCol());

      case T_INT_ARBITRARY:
        return new ServerArbitraryIntRow(rowIndex, partitionKey.getStartCol(),
            partitionKey.getEndCol());

      case T_FLOAT_SPARSE:
        return  new ServerSparseFloatRow(rowIndex, partitionKey.getStartCol(), partitionKey.getStartCol());

      default:
        LOG.warn("invalid rowtype " + rowType + ", default is " + RowType.T_DOUBLE_DENSE);
        return new ServerDenseDoubleRow(rowIndex, partitionKey.getStartCol(),
            partitionKey.getEndCol());
    }
  }

  private ServerRow initRow(RowType rowType) {
    switch (rowType) {
      case T_DOUBLE_DENSE:
        return new ServerDenseDoubleRow();

      case T_DOUBLE_SPARSE:
        return new ServerSparseDoubleRow();

      case T_INT_DENSE:
        return new ServerDenseIntRow();

      case T_FLOAT_DENSE:
        return new ServerDenseFloatRow();

      case T_INT_SPARSE:
        return new ServerSparseIntRow();

      case T_INT_ARBITRARY:
        return new ServerArbitraryIntRow();

      default:
        LOG.warn("invalid rowtype " + rowType + ", default is " + RowType.T_DOUBLE_DENSE);
        return new ServerDenseDoubleRow();
    }
  }

  public RowType getRowType() {
    return rowType;
  }

  /**
   * Read rows of partition from input
   *
   * @param input the input
   * @throws IOException
   */
  public void readFrom(DataInputStream input) throws IOException {
    readClocks(input);
    int size = input.readInt();
    if (LOG.isDebugEnabled()) {
      LOG.debug("read partitionKey: " + partitionKey);
      LOG.debug("row size: " + size);
    }
    for (int i = 0; i < size; i++) {
      int rowId = input.readInt();
      if (LOG.isDebugEnabled()) {
        LOG.info("rowId: " + rowId);
      }
      rows.get(rowId).readFrom(input);
    }
  }
  
  private void readClocks(DataInputStream input) throws IOException{
    try {
      lock.writeLock().lock();
      LOG.debug("readClocks, partition " + partitionKey + " clock details:");      
      minClock = input.readInt();
      LOG.debug("minClock=" + minClock);
      int clockMapSize = input.readInt();
      for(int i = 0; i < clockMapSize; i++){
        int taskId = input.readInt();
        int clock = input.readInt();
        LOG.debug("taskId=" + taskId + ", clock=" + clock);
        taskIndexToClockMap.put(taskId, clock);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Write rows of partition to output
   *
   * @param output the output
   * @throws IOException
   */
  public void writeTo(DataOutputStream output) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("write partitionKey: " + partitionKey);
      LOG.debug("row size: " + rows.size());
    }
    
    writeClocks(output);
    
    output.writeInt(rows.size()); // write row size
    for (Map.Entry<Integer, ServerRow> entry : rows.entrySet()) {
      output.writeInt(entry.getKey()); // write rowId
      entry.getValue().writeTo(output); // write rowContent
    }
  }
  
  private void writeClocks(DataOutputStream output) throws IOException{
    try {
      lock.readLock().lock();
      LOG.debug("writeClocks, partition " + partitionKey + " clock details:");
      LOG.debug("minClock=" + minClock);
      output.writeInt(minClock);
      output.writeInt(taskIndexToClockMap.size());
      for(Entry clockEntry:taskIndexToClockMap.int2IntEntrySet()){
        LOG.debug("taskId=" + clockEntry.getIntKey() + ", clock=" + clockEntry.getIntValue());
        output.writeInt(clockEntry.getIntKey());
        output.writeInt(clockEntry.getIntValue());
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Reset rows.
   */
  public void reset() {
    rows.clear();
    initRows(partitionKey, rowType);
  }

  /**
   * Commit partition.
   *
   * @param output the output
   * @throws IOException the io exception
   */
  public void commit(DataOutputStream output) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("commit partitionKey: " + partitionKey);
      LOG.debug("row size: " + rows.size());
    }
    // start end
    partitionKey.write(output);
    // rowtype
    output.writeUTF(rowType.toString());
    output.writeInt(rows.size()); // write row size
    for (Map.Entry<Integer, ServerRow> entry : rows.entrySet()) {
      output.writeInt(entry.getKey()); // write rowId
      ServerRow row = entry.getValue();
      row.writeTo(output); // write rowContent
    }
  }

  /**
   * Load partition.
   *
   * @param input the input
   * @throws IOException
   */
  public void load(DataInputStream input) throws IOException {
    PartitionKey pkey = new PartitionKey(-1, -1, -1, -1, -1, -1);
    pkey.read(input);

    if (pkey.getStartRow() != partitionKey.getStartRow() ||
            pkey.getEndRow() != partitionKey.getEndRow() ||
            pkey.getStartCol() != partitionKey.getStartCol() ||
            pkey.getEndCol() != partitionKey.getEndCol()) {
      LOG.error("Load error " + pkey + " while " + partitionKey);
      throw new IOException("Load Error " + pkey + " while " + partitionKey);
    }

    String rowType = input.readUTF();
    LOG.info("RowType for partition " + partitionKey + " is " + rowType);

    int size = input.readInt();
    for (int i = 0; i < size; i ++) {
      int rowId = input.readInt();
      ServerRow serverRow = rows.get(rowId);
      serverRow.readFrom(input);
    }
  }

  @Override
  public void serialize(ByteBuf buf) {
    if (partitionKey == null) {
      return;
    }

    partitionKey.serialize(buf);
    buf.writeInt(rowType.getNumber());
    buf.writeInt(minClock);
    buf.writeInt(rows.size());
    for (java.util.Map.Entry<Integer, ServerRow> rowEntry : rows.entrySet()) {
      rowEntry.getValue().serialize(buf);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    partitionKey = new PartitionKey();
    partitionKey.deserialize(buf);
    rowType = RowType.valueOf(buf.readInt());
    minClock = buf.readInt();
    int rowNum = buf.readInt();
    for (int i = 0; i < rowNum; i++) {
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

  public List<Integer> getRowIds() {
    Enumeration<Integer> iter = this.rows.keys();
    List<Integer> rowIds = new ArrayList<Integer>();
    while(iter.hasMoreElements()) {
      rowIds.add(iter.nextElement());
    }
    return rowIds;
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
}
