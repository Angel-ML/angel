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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.PartitionKey;
import io.netty.buffer.ByteBuf;

/**
 * The request that Update task clock in matrix partition
 */
public class UpdateClockRequest extends PartitionRequest {
  /**
   * Task index
   */
  private int taskIndex;

  /**
   * Create a UpdateClockRequest
   * @param partKey partition key
   * @param taskIndex task index
   * @param clock clock value
   */
  public UpdateClockRequest(PartitionKey partKey, int taskIndex, int clock) {
    super(clock, partKey);
    this.taskIndex = taskIndex;
  }

  /**
   * Create a UpdateClockRequest, just for serialize/deserialize
   */
  public UpdateClockRequest() {

  }

  @Override public int getEstimizeDataSize() {
    return bufferLen();
  }

  @Override public TransportMethod getType() {
    return TransportMethod.UPDATE_CLOCK;
  }

  /**
   * Get task index
   * @return task index
   */
  public int getTaskIndex() {
    return taskIndex;
  }

  /**
   * Set task index
   * @param taskIndex task index
   */
  public void setTaskIndex(int taskIndex) {
    this.taskIndex = taskIndex;
  }


  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(taskIndex);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    taskIndex = buf.readInt();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4;
  }

  @Override public String toString() {
    return "UpdateClockRequest{" + "taskIndex=" + taskIndex + "} " + super.toString();
  }
}
