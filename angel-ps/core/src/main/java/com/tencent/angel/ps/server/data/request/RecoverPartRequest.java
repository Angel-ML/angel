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


package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.server.data.TransportMethod;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;


/**
 * Partition data recover request
 */
public class RecoverPartRequest extends PartitionRequest {
  /**
   * Partition data
   */
  private ServerPartition part;

  /**
   * Task clock vector
   */
  private Int2IntOpenHashMap taskIndexToClockMap;

  /**
   * Create a RecoverPartRequest
   *
   * @param taskIndexToClockMap task index to clock value map
   * @param partKey             partition key
   * @param part                partition data
   */
  public RecoverPartRequest(Int2IntOpenHashMap taskIndexToClockMap, PartitionKey partKey,
    ServerPartition part) {
    super(0, partKey);
    this.taskIndexToClockMap = taskIndexToClockMap;
    this.part = part;
  }

  /**
   * Create a RecoverPartRequest
   *
   * @param partKey partition key
   * @param part    partition data
   */
  public RecoverPartRequest(PartitionKey partKey, ServerPartition part) {
    this(null, partKey, part);
  }

  /**
   * Create a RecoverPartRequest
   */
  public RecoverPartRequest() {

  }

  @Override public int getEstimizeDataSize() {
    return 0;
  }

  @Override public TransportMethod getType() {
    return TransportMethod.RECOVER_PART;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    part.serialize(buf);
    if (taskIndexToClockMap != null) {
      buf.writeInt(taskIndexToClockMap.size());
      ObjectIterator<Int2IntMap.Entry> iter = taskIndexToClockMap.int2IntEntrySet().fastIterator();
      Int2IntMap.Entry item;
      while (iter.hasNext()) {
        item = iter.next();
        buf.writeInt(item.getIntKey());
        buf.writeInt(item.getIntValue());
      }
    } else {
      buf.writeInt(0);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    part = new ServerPartition();
    part.deserialize(buf);
    int clockVecSize = buf.readInt();
    if (clockVecSize > 0) {
      taskIndexToClockMap = new Int2IntOpenHashMap(clockVecSize);
      for (int i = 0; i < clockVecSize; i++) {
        taskIndexToClockMap.put(buf.readInt(), buf.readInt());
      }
    }
  }

  @Override public int bufferLen() {
    return super.bufferLen() + ((part != null) ? part.bufferLen() : 0) + 4 + ((taskIndexToClockMap
      != null) ? taskIndexToClockMap.size() * 8 : 0);
  }

  /**
   * Get partition data
   *
   * @return partition data
   */
  public ServerPartition getPart() {
    return part;
  }

  /**
   * Get clock vector for this partition
   *
   * @return clock vector
   */
  public Int2IntOpenHashMap getTaskIndexToClockMap() {
    return taskIndexToClockMap;
  }

  @Override public String toString() {
    return "RecoverPartRequest{" + "part=" + part + ", taskIndexToClockMap=" + toString(
      taskIndexToClockMap) + "} " + super.toString();
  }

  private String toString(Int2IntOpenHashMap clocVec) {
    if (clocVec == null) {
      return "NULL";
    }

    StringBuilder sb = new StringBuilder();
    ObjectIterator<Int2IntMap.Entry> iter = clocVec.int2IntEntrySet().fastIterator();
    Int2IntMap.Entry item;
    while (iter.hasNext()) {
      item = iter.next();
      sb.append(item.getIntKey());
      sb.append(":");
      sb.append(item.getIntValue());
      sb.append(";");
    }
    return sb.toString();
  }
}
